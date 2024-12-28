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
    def __init__(self, location: str):
        self.location = location  # Memory, Local File/DB, or Remote File/DB

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
## Memory ResultIO and IResultOperations

class MemoryResultIO(ResultIO):
    def __init__(self):
        # May be used for writing results to disk later
        root_temp_dir = Path(tempfile.gettempdir()).resolve()
        location: str = str(root_temp_dir / f"memory-{uuid.uuid4()}")

        super().__init__(location)

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

#=============================================================================================
## Local ResultIO and IResultOperations


#=============================================================================================
## TODO: Remote ResultIO and IResultOperations using SSH protocol
