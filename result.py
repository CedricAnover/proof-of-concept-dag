import json
import os
import shutil
import tempfile
import uuid
from pathlib import Path
from typing import AnyStr, Any, Optional, Type, get_type_hints, get_args, get_origin
from dataclasses import dataclass, fields, is_dataclass
from abc import ABC, ABCMeta, abstractmethod


@dataclass
class Result(ABC):
    @classmethod
    @abstractmethod
    def deserialize(cls, obj_str: AnyStr, *args, **kwargs) -> "Result":
        pass

    @abstractmethod
    def serialize(self, *args, **kwargs) -> AnyStr:
        pass


class DictMeta(ABCMeta):
    def __new__(mcls, name: str, bases: tuple, attrs: dict):
        # Add `to_dict` and `from_dict` methods
        attrs["to_dict"] = mcls.to_dict
        attrs["from_dict"] = classmethod(mcls.from_dict)

        return super().__new__(mcls, name, bases, attrs)

    @staticmethod
    def _is_dataclass_instance(obj):
        """Check if an object is a dataclass instance."""
        return is_dataclass(obj) and not isinstance(obj, type)

    @staticmethod
    def _from_dict_recursive(cls: Type[Any], data: Any) -> Any:
        """
        Recursively convert dictionary data to dataclass instances.
        """
        if isinstance(data, dict):
            if is_dataclass(cls):
                # Handle nested dataclass for dictionary fields
                init_kwargs = {}
                type_hints = get_type_hints(cls)
                for field_name, field_value in data.items():
                    field_type = type_hints.get(field_name, Any)
                    if DictMeta._is_dataclass_instance(field_type):
                        init_kwargs[field_name] = field_type.from_dict(field_value)
                    elif get_origin(field_type) == dict:
                        key_type, value_type = get_args(field_type)
                        init_kwargs[field_name] = {
                            key_type(k): DictMeta._from_dict_recursive(value_type, v)
                            for k, v in field_value.items()
                        }
                    elif get_origin(field_type) == list:
                        inner_type = get_args(field_type)[0]
                        init_kwargs[field_name] = [
                            DictMeta._from_dict_recursive(inner_type, v)
                            for v in field_value
                        ]
                    else:
                        init_kwargs[field_name] = field_value
                return cls(**init_kwargs)
            return data
        elif isinstance(data, list):
            # Handle lists of dataclasses
            return [DictMeta._from_dict_recursive(cls, item) for item in data]
        return data

    @staticmethod
    def _to_dict_recursive(obj: Any) -> Any:
        """
        Recursively convert dataclass instances to dictionaries.
        """
        if DictMeta._is_dataclass_instance(obj):
            return {
                field.name: DictMeta._to_dict_recursive(getattr(obj, field.name))
                for field in fields(obj)
            }
        elif isinstance(obj, list):
            return [DictMeta._to_dict_recursive(item) for item in obj]
        elif isinstance(obj, dict):
            return {str(k): DictMeta._to_dict_recursive(v) for k, v in obj.items()}
        return obj

    @staticmethod
    def from_dict(cls: Type["Result"], data: dict) -> "Result":
        """
        Recursively convert a dictionary to a dataclass instance.
        """
        return DictMeta._from_dict_recursive(cls, data)

    @staticmethod
    def to_dict(self) -> dict:
        """
        Recursively convert a dataclass instance to a dictionary.
        """
        return DictMeta._to_dict_recursive(self)


@dataclass
class JsonResult(Result, metaclass=DictMeta):
    @classmethod
    def deserialize(cls: Type["JsonResult"], obj_str: str, *args, **kwargs) -> "JsonResult":
        """
        Deserialize JSON string to an instance of the class, handling nested dataclasses.
        """
        dct = json.loads(obj_str, *args, **kwargs)
        return cls.from_dict(dct)

    def serialize(self, *args, **kwargs) -> str:
        """
        Serialize the dataclass to JSON, handling nested dataclasses.
        """
        return json.dumps(self.to_dict(), *args, **kwargs)


class ResultIO(ABC):
    def __init__(self, temp_location: Optional[str] = None, name_prefix: str = "dag"):
        temp_dir_name = f"{name_prefix}-{uuid.uuid4()}"
        root_temp_dir = Path(tempfile.gettempdir()).resolve()

        # Defaults locally if temp_location is not provided (locally or remotely)
        self.temp_location = temp_location or str(root_temp_dir / temp_dir_name)

    @abstractmethod
    def write_result(self, result: Result, node_label: str, *args, **kwargs) -> None:
        pass

    @abstractmethod
    def read_result(self, node_label: str, result_kind: type[Result], *args, **kwargs) -> Result:
        pass


class MemoryResultIO(ResultIO):
    def __init__(self):
        super().__init__("")
        self._result_storage = dict()  # In memory

    def write_result(self, result: Result, node_label: str) -> None:
        self._result_storage[node_label] = result

    def read_result(self, node_label: str, result_kind: type[Result]) -> Result:
        return self._result_storage[node_label]


class LocalFsCrudMeta(ABCMeta):
    def __new__(mcls, name: str, bases: tuple, attrs: dict):
        assert ResultIO in bases, "ResultIO is not inherited."

        attrs["read_results"] = mcls.read_results
        attrs["create_temp_location"] = mcls.create_temp_location
        attrs["delete_temp_location"] = mcls.delete_temp_location
        attrs["file_path"] = mcls.file_path
        attrs["transfer_results"] = mcls.transfer_results

        new_cls = super().__new__(mcls, name, bases, attrs)
        return new_cls

    @staticmethod
    def read_results(self, node_labels: list[str], *args, **kwargs) -> list[Result]:
        return [self.read_result(node_label, *args, **kwargs) for node_label in node_labels]

    @staticmethod
    def create_temp_location(self, *args, **kwargs) -> None:
        os.makedirs(self.temp_location, *args, **kwargs)

    @staticmethod
    def delete_temp_location(self, *args, ignore_errors=True, **kwargs) -> None:
        shutil.rmtree(self.temp_location, *args, ignore_errors=ignore_errors, **kwargs)

    @staticmethod
    def transfer_results(self, destination_dir: str) -> None:
        dest_dir_path = Path(destination_dir).resolve()
        src_dir_path = Path(self.temp_location).resolve()

        if not dest_dir_path.exists():
            os.makedirs(str(dest_dir_path))

        if not dest_dir_path.is_dir():
            raise ValueError("The given destination directory is not a directory.")

        shutil.move(src_dir_path, dest_dir_path)

    @staticmethod
    def file_path(self, node_label: str, file_extension: str | None = None) -> Path:
        temp_dir_path = Path(self.temp_location).resolve()
        if file_extension:
            return temp_dir_path / f"{node_label}.{file_extension}"
        else:
            return temp_dir_path / f"{node_label}"


class LocalResultIO(ResultIO, metaclass=LocalFsCrudMeta):
    file_extension: str = "json"

    def write_result(self, result: Result, node_label: str) -> None:
        file_path = self.file_path(node_label, file_extension=self.file_extension)
        if not file_path.parent.exists():
            file_path.parent.mkdir(parents=True, exist_ok=True)
        file_path.write_text(result.serialize())

    def read_result(self, node_label: str, result_kind: type[Result]) -> Result:
        file_path = self.file_path(node_label, file_extension=self.file_extension)
        obj_str = file_path.read_text()
        result = result_kind.deserialize(obj_str)
        return result
