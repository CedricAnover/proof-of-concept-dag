import os
import shutil
import tempfile
import uuid
from pathlib import Path
from typing import Any, Optional
from abc import ABC, abstractmethod
from pydantic import BaseModel


class Result(BaseModel):
    node_label: str
    is_success: bool
    result_data: Optional[Any] = None
    error: Optional[str] = None

    @classmethod
    def create_success_result(cls, node_label: str, result_data: Any, *args, **kwargs) -> "Result":
        return cls(node_label=node_label, is_success=True, result_data=result_data, *args, **kwargs)

    @classmethod
    def create_fail_result(cls, node_label: str, error_message: str, *args, **kwargs) -> "Result":
        return cls(node_label=node_label, is_success=False, error=error_message, *args, **kwargs)

    @classmethod
    def deserialize(cls, json_str: str, *args, **kwargs) -> "Result":
        return cls.model_validate_json(json_str, *args, **kwargs)

    def serialize(self, *args, **kwargs) -> str:
        return self.model_dump_json(*args, **kwargs)


class ResultIO(ABC):
    def __init__(self, location: str):
        self.location = location

    @abstractmethod
    def write_result(self, result: Result, node_label: str, *args, **kwargs) -> None:
        pass

    @abstractmethod
    def read_result(self, node_label: str, *args, **kwargs) -> Result:
        pass


class MemoryResultIO(ResultIO):
    def __init__(self):
        super().__init__("")
        self._result_storage = dict()  # In memory; Node Label => Result

    @property
    def results(self) -> list[Result]:
        return [result for _, result in self._result_storage.items()]

    def write_result(self, result: Result, node_label: str) -> None:
        self._result_storage[node_label] = result

    def read_result(self, node_label: str) -> Result:
        return self._result_storage[node_label]


class LocalResultIO(ResultIO):
    file_extension: str = "json"

    def __init__(self, location: Optional[str] = None, name_prefix: str = "dag"):
        dir_name = f"{name_prefix}-{uuid.uuid4()}"
        root_dir = Path(tempfile.gettempdir()).resolve()

        # Defaults to local temporary directory
        location_ = location or str(root_dir / dir_name)
        super().__init__(location_)

    def write_result(self, result: Result, node_label: str) -> None:
        file_path = self.file_location(node_label)
        if not file_path.parent.exists():
            file_path.parent.mkdir(parents=True, exist_ok=True)
        file_path.write_text(result.serialize())

    def read_result(self, node_label: str) -> Result:
        file_path = self.file_location(node_label)
        obj_str = file_path.read_text()
        result = Result.deserialize(obj_str)
        return result

    def create_location_directory(self, *args, **kwargs) -> None:
        os.makedirs(self.location, *args, **kwargs)

    def delete_location_directory(self, ignore_errors=True, *args, **kwargs) -> None:
        shutil.rmtree(self.location, *args, ignore_errors=ignore_errors, **kwargs)

    def transfer_results(self, destination_location: str) -> None:
        dest_dir_path = Path(destination_location).resolve()
        src_dir_path = Path(self.location).resolve()

        if not dest_dir_path.exists():
            os.makedirs(str(dest_dir_path))

        if not dest_dir_path.is_dir():
            raise ValueError("The given destination directory is not a directory.")

        shutil.move(src_dir_path, dest_dir_path)

    def file_location(self, node_label) -> Path:
        location_dir_path = Path(self.location).resolve()
        if self.file_extension:
            return location_dir_path / f"{node_label}.{self.file_extension}"
        else:
            return location_dir_path / f"{node_label}"
