import os
import shutil
import tempfile
import uuid
from pathlib import Path
from typing import AnyStr, Any, Optional
from abc import ABC, abstractmethod

from pydantic import BaseModel


class Result(BaseModel):
    node_label: str
    is_success: bool
    data: Optional[Any] = None
    error: Optional[str] = None

    @classmethod
    def create_success_result(cls, node_label: str, data: Any, *args, **kwargs) -> "Result":
        return cls(node_label=node_label, is_success=True, data=data, *args, **kwargs)

    @classmethod
    def create_fail_result(cls, node_label: str, error_message: str, *args, **kwargs) -> "Result":
        return cls(node_label=node_label, is_success=False, error=error_message, *args, **kwargs)

    @classmethod
    def deserialize(cls, result_json_str: AnyStr, *args, **kwargs) -> "Result":
        return cls.model_validate_json(result_json_str, *args, **kwargs)

    def serialize(self, *args, **kwargs) -> str:
        return self.model_dump_json(*args, **kwargs)


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


class IFileSystemOperations(ABC):
    # FS Operations for either local or remote file systems
    @abstractmethod
    def create_temp_location(self, *args, **kwargs) -> None:
        pass

    @abstractmethod
    def delete_temp_location(self, *args, **kwargs) -> None:
        pass

    @abstractmethod
    def transfer_results(self, destination_location: str, *args, **kwargs) -> None:
        pass

    @abstractmethod
    def file_location(self, node_label: str, *args, **kwargs) -> str | Path:
        pass


class MemoryResultIO(ResultIO):
    def __init__(self):
        super().__init__("")
        self._result_storage = dict()  # In memory

    def write_result(self, result: Result, node_label: str) -> None:
        self._result_storage[node_label] = result

    def read_result(self, node_label: str, result_kind: type[Result]) -> Result:
        return self._result_storage[node_label]


class LocalResultIO(ResultIO, IFileSystemOperations):
    file_extension: str = "json"

    def write_result(self, result: Result, node_label: str) -> None:
        file_path = self.file_location(node_label)
        if not file_path.parent.exists():
            file_path.parent.mkdir(parents=True, exist_ok=True)
        file_path.write_text(result.serialize())

    def read_result(self, node_label: str, result_kind: type[Result]) -> Result:
        file_path = self.file_location(node_label)
        obj_str = file_path.read_text()
        result = result_kind.deserialize(obj_str)
        return result

    def create_temp_location(self, *args, **kwargs) -> None:
        os.makedirs(self.temp_location, *args, **kwargs)

    def delete_temp_location(self, ignore_errors=True, *args, **kwargs) -> None:
        shutil.rmtree(self.temp_location, *args, ignore_errors=ignore_errors, **kwargs)

    def transfer_results(self, destination_location: str) -> None:
        dest_dir_path = Path(destination_location).resolve()
        src_dir_path = Path(self.temp_location).resolve()

        if not dest_dir_path.exists():
            os.makedirs(str(dest_dir_path))

        if not dest_dir_path.is_dir():
            raise ValueError("The given destination directory is not a directory.")

        shutil.move(src_dir_path, dest_dir_path)

    def file_location(self, node_label) -> Path:
        temp_dir_path = Path(self.temp_location).resolve()
        if self.file_extension:
            return temp_dir_path / f"{node_label}.{self.file_extension}"
        else:
            return temp_dir_path / f"{node_label}"
