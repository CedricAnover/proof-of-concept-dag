import os
import shutil
import tempfile
import uuid
import pickle
from pathlib import Path
from typing import Any, AnyStr, Type, Optional, Sequence, Protocol
from abc import ABC, abstractmethod
from pydantic import BaseModel, Field


class ResultError(Exception):
    """Base class of all result related errors."""


class ResultIOError(ResultError):
    pass


class ResultData(BaseModel, Protocol):
    """Base class for all result data."""


class Result(BaseModel):
    node_label: str = Field(..., description="Node label associated with the result.")
    is_success: bool = Field(..., description="Completion state of a node (Sucess or Fail).")
    result_data: Optional[ResultData] = Field(None, description="Output result of the node. Note that this should be serializable/deserializable.")
    error: Optional[str] = Field(None, description="Error message if an error occurred after running a node.")
    id_: uuid.UUID = Field(default_factory=uuid.uuid4, description="A unique identifier for the result.")


class ISerializeDeserialize(ABC):
    @abstractmethod
    def serialize(self, result: Result, *args, **kwargs) -> AnyStr:
        pass

    @abstractmethod
    def deserialize(self, result_str: AnyStr, *args, **kwargs) -> Result:
        pass


class ResultIO(ABC):
    # The optionality of Pickle, JSON, CSV, etc. has to be decided & implemented here.
    def __init__(self, location: str, serializer: ISerializeDeserialize):
        self.location = location  # Memory, Local File/DB, or Remote File/DB
        self.serializer = serializer

    @abstractmethod
    def write_result(self, result: Result, node_label: AnyStr, *args, **kwargs) -> None:
        pass

    @abstractmethod
    def read_result(self, node_label: AnyStr, *args, **kwargs) -> Result:
        pass


class IResultOperations(ABC):
    def __init__(self, result_io: ResultIO):
        self.result_io = result_io

    @abstractmethod
    def create_location(self, *args, **kwargs) -> None:
        pass

    @abstractmethod
    def delete_location(self, *args, **kwargs) -> None:
        pass

    @abstractmethod
    def transfer_results(self, dest_location: AnyStr | Path, *args, **kwargs) -> None:
        pass

    @abstractmethod
    def result_location(self, node_label: AnyStr, *args, **kwargs) -> AnyStr | Path:
        pass

#=============================================================================================
## JSON

class JsonSerializer(ISerializeDeserialize):
    def serialize(self, result: Result, *args, **kwargs) -> str:
        return result.model_dump_json(*args, **kwargs)

    def deserialize(self, result_str: str, *args, **kwargs) -> Result:
        return Result.model_validate_json(result_str, *args, **kwargs)

#=============================================================================================
## Pickle

class PickleSerializer(ISerializeDeserialize):
    def serialize(self, result: Result, *args, **kwargs) -> bytes:
        return pickle.dumps(result, *args, **kwargs)

    def deserialize(self, result_str: bytes, *args, **kwargs) -> Result:
        return pickle.loads(result_str)

#=============================================================================================

def _get_file_extension(serializer: ISerializeDeserialize) -> str:
    """Retuns the file extension based on the given serializer."""
    if not isinstance(serializer, ISerializeDeserialize):
        raise ResultIO("serializer must be a subclass of ISerializeDeserialize")

    if isinstance(serializer, JsonSerializer):
        return "json"

    if isinstance(serializer, PickleSerializer):
        return "pkl"

#=============================================================================================
## Memory ResultIO and IResultOperations

class MemoryResultIO(ResultIO):
    def __init__(self, location: Optional[str] = None, serializer: Optional[ISerializeDeserialize] = JsonSerializer()):
        # May be used for writing results to disk later
        root_temp_dir = Path(tempfile.gettempdir()).resolve()
        location = location or str(root_temp_dir / f"memory-{uuid.uuid4()}")

        super().__init__(location, serializer)

        # Memory Storage using Dictionary
        self._memory_store = {}

    @property
    def memory_store(self) -> dict[str, Result]:
        return self._memory_store

    def write_result(self, result: Result, node_label: AnyStr) -> None:
        """Write the result to the in-memory store."""
        self._memory_store[node_label] = result

    def read_result(self, node_label: AnyStr) -> Result:
        """Read the result from the in-memory store."""
        if node_label not in self._memory_store:
            raise ResultIOError(f"No result found for node_label '{node_label}'")

        return self._memory_store[node_label]


class MemoryResultOperations(IResultOperations):
    def __init__(self, result_io: MemoryResultIO):
        if not isinstance(result_io, MemoryResultIO):
            raise ResultIOError("result_io must be a MemoryResultIO.")
        super().__init__(result_io)

    def create_location(self) -> None:
        Path(self.result_io.location).resolve().mkdir(parents=True, exist_ok=True)

    def delete_location(self) -> None:
        location_dir = str(Path(self.result_io.location).resolve())
        shutil.rmtree(location_dir, ignore_errors=True)

    def transfer_results(self, dest_location: str) -> None:
        src_dir_path = Path(self.result_io.location).resolve()
        dest_dir_path = Path(dest_location).resolve()  # Destination location must be a local directory

        if not dest_dir_path.exists():
            dest_dir_path.mkdir(parents=True, exist_ok=True)
        assert dest_dir_path.is_dir(), f"{dest_dir_path} is not a directory."

        # Create the temporary location (overhead)
        self.create_location()

        for node_label, result in self.result_io.memory_store.items():
            file_path = self.result_location(node_label)
            content = self.result_io.serializer.serialize(result)
            if isinstance(self.result_io.serializer, JsonSerializer):
                file_path.write_text(content)
            if isinstance(self.result_io.serializer, PickleSerializer):
                file_path.write_bytes(content)

        # Move result files from temporary result directory to custom directory
        shutil.move(src_dir_path, dest_dir_path)

        # Delete the temporary result directory
        self.delete_location()

    def result_location(self, node_label: str) -> Path:
        location_dir = Path(self.result_io.location).resolve()
        file_extension = _get_file_extension(self.result_io.serializer)
        file_path = location_dir / f"{node_label}.{file_extension}"
        return file_path

#=============================================================================================
## Local ResultIO and IResultOperations

class LocalResultIO(ResultIO):
    def write_result(self, result: Result, node_label: AnyStr, *args, **kwargs) -> None:
        ...

    def read_result(self, node_label: AnyStr, *args, **kwargs) -> Result:
        return ...


class LocalResultOperations(IResultOperations):
    def __init__(self, location: str):
        result_io = LocalResultIO(location)
        super().__init__(result_io)

    def create_location(self, *args, **kwargs):
        ...
    
    def delete_location(self, *args, **kwargs):
        ...
    
    def transfer_results(self, dest_location, *args, **kwargs):
        ...
    
    def result_location(self, node_label, *args, **kwargs):
        return ...

#=============================================================================================
## TODO: Remote ResultIO and IResultOperations (e.g. SFTP)
