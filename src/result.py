import os
import shutil
import tempfile
import uuid
from pathlib import Path
from typing import Any, AnyStr, Type, Optional, Sequence, Protocol
from abc import ABC, abstractmethod
from pydantic import BaseModel, Field


class Result(BaseModel):
    node_label: str = Field(..., description="Node label associated with the result.")
    is_success: bool = Field(..., description="Completion state of a node (Sucess or Fail).")
    result_data: Optional[Any] = Field(None, description="Output result of the node. Note that this should be serializable/deserializable.")
    error: Optional[str] = Field(None, description="Error message if an error occurred after running a node.")
    id_: uuid.UUID = Field(default_factory=uuid.uuid4, description="A unique identifier for the result.")


class ISerializeDeserialize(ABC):
    @abstractmethod
    def serialize(self, result: Result, *args, **kwargs) -> AnyStr:
        pass

    @classmethod
    @abstractmethod
    def deserialize(cls, result_str: AnyStr, *args, **kwargs) -> Result:
        pass


class ResultIO(ABC):
    # The optionality of Pickle, JSON, CSV, etc. has to be decided & implemented here.
    def __init__(self, location: str, ):
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
    def transfer_results(self, dest_location: AnyStr, *args, **kwargs) -> None:
        pass

    @abstractmethod
    def result_location(self, node_label: AnyStr, *args, **kwargs) -> AnyStr | Path:
        pass

#=============================================================================================

