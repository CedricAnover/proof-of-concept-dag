import json
import os
import shutil
import tempfile
import uuid
from pathlib import Path
from typing import AnyStr, Optional
from dataclasses import dataclass, asdict
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


class JsonResultMeta(ABCMeta):
    # Important: We assume that the dictionary is JSON Serializable!
    def __new__(mcls, name: str, bases: tuple, attrs: dict):
        # Add to_json() and from_json() methods to a subclass of Result
        attrs["to_json"] = mcls.to_json
        attrs["from_json"] = classmethod(mcls.from_json)

        new_cls = super().__new__(mcls, name, bases, attrs)
        return new_cls

    @staticmethod
    def to_json(self, *args, **kwargs) -> str:
        # Assert that `self` is a Result (i.e. dataclass)
        assert isinstance(self, Result)
        return json.dumps(asdict(self), *args, **kwargs)

    @staticmethod
    def from_json(cls, json_str: str, *args, **kwargs) -> Result:
        dct = dict(json.loads(json_str, *args, **kwargs))
        return cls(**dct)


@dataclass
class JsonResult(Result):
    @classmethod
    def deserialize(cls, obj_str: str, *args, **kwargs) -> "JsonResult":
        dct = json.loads(obj_str, *args, **kwargs)
        return cls(**dct)

    def serialize(self, *args, **kwargs) -> str:
        return json.dumps(asdict(self), *args, **kwargs)


class ResultIO(ABC):
    def __init__(self, temp_location: Optional[str] = None, name_prefix: str = "dag"):
        temp_dir_name = f"{name_prefix}-{uuid.uuid4()}"
        root_temp_dir = Path(tempfile.gettempdir()).resolve()
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

        # Flag if results need to be transfered to
        # another directory before deletion.
        attrs["use_transfer_results"] = False

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
            # By default, the user must create the directory.
            raise FileNotFoundError("The directory for transfering results does not exist.")

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
        file_path.write_text(result.serialize())

    def read_result(self, node_label: str, result_kind: type[Result]) -> Result:
        file_path = self.file_path(node_label, file_extension=self.file_extension)
        obj_str = file_path.read_text()
        result = result_kind.deserialize(obj_str)
        return result
