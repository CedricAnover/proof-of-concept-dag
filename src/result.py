import uuid
import shutil
import tempfile
import uuid
import pickle
from pathlib import Path
from typing import Any, Optional, Dict, Protocol, Union
from abc import ABC, abstractmethod
from pydantic import BaseModel, Field

from .exceptions import ResultDataError, ResultIOError, ResultNotFoundError


class Result(BaseModel):
    """Represents a result including node metadata and associated result data."""
    node_label: str = Field(..., description="Node label associated with the result.")
    is_success: bool = Field(..., description="Completion state of a node (Success or Fail).")
    result_data: Optional[Any] = Field(None, description="Output result of the node.")
    error: Optional[str] = Field(None, description="Error message if an error occurred.")
    id_: uuid.UUID = Field(default_factory=uuid.uuid4, description="A unique identifier for the result.")

    def validate(self) -> None:
        """Ensure result consistency (e.g., `error` should be None if `is_success` is True)."""
        if self.is_success and self.error:
            raise ResultDataError(f"Success result cannot have an error message.")
        if not self.is_success and not self.error:
            raise ResultDataError(f"Failed result must have an error message.")


class ISerializeDeserialize(ABC):
    @property
    @abstractmethod
    def file_extension(self) -> str:
        """Returns the file extension without a dot."""
        pass

    @abstractmethod
    def serialize(self, result: Result) -> Union[str, bytes]:
        """Serializes the result to a string or byte format."""
        pass

    @abstractmethod
    def deserialize(self, data: Union[str, bytes]) -> Result:
        """Deserializes data to a Result object."""
        pass


class ResultIO(ABC):
    """Abstract class for handling I/O operations for results."""
    def __init__(self, location: str, serializer: ISerializeDeserialize):
        self.location = location
        self.serializer = serializer

    @abstractmethod
    def write_result(self, result: Result) -> None:
        """Write result to storage."""
        pass

    @abstractmethod
    def read_result(self, node_label: str) -> Result:
        """Read result from storage."""
        pass


class MemoryResultIO(ResultIO):
    """In-memory Result I/O implementation."""
    def __init__(self, location: Optional[str] = None, serializer: Optional[ISerializeDeserialize] = None):
        location = location or f"memory-{uuid.uuid4()}"
        serializer = serializer or JsonSerializer()
        super().__init__(location, serializer)
        self._memory_store: Dict[str, Result] = {}

    def write_result(self, result: Result) -> None:
        """Write result to in-memory store."""
        self._memory_store[result.node_label] = result

    def read_result(self, node_label: str) -> Result:
        """Retrieve result from memory store."""
        try:
            return self._memory_store[node_label]
        except KeyError:
            raise ResultNotFoundError(f"Result not found for node: {node_label}")


class LocalResultIO(ResultIO):
    """Local file system result I/O implementation."""
    def write_result(self, result: Result) -> None:
        """Write result to a local file."""
        location_dir = Path(self.location).resolve()
        file_path = location_dir / f"{result.node_label}.{self.serializer.file_extension}"

        # Ensure directory exists
        if not location_dir.exists():
            location_dir.mkdir(parents=True, exist_ok=True)

        content = self.serializer.serialize(result)

        # Write to file
        try:
            with open(file_path, 'wb' if self.serializer.file_extension == "pkl" else 'w') as f:
                f.write(content)
        except Exception as e:
            raise ResultIOError(f"Error writing result to {file_path}: {e}")

    def read_result(self, node_label: str) -> Result:
        """Read result from a local file."""
        location_dir = Path(self.location).resolve()
        file_path = location_dir / f"{node_label}.{self.serializer.file_extension}"

        # Check if the file exists
        if not file_path.exists():
            raise ResultNotFoundError(f"Result file not found: {file_path}")

        try:
            with open(file_path, 'rb' if self.serializer.file_extension == "pkl" else 'r') as f:
                data = f.read()
            return self.serializer.deserialize(data)
        except Exception as e:
            raise ResultIOError(f"Error reading result from {file_path}: {e}")


class IResultOperations(ABC):
    """Abstract class for operations related to result management."""
    def __init__(self, result_io: ResultIO):
        self.result_io = result_io

    @abstractmethod
    def create_location(self) -> None:
        """Create the result storage location."""
        pass

    @abstractmethod
    def delete_location(self) -> None:
        """Delete the result storage location."""
        pass

    @abstractmethod
    def transfer_results(self, dest_location: str) -> None:
        """Transfer results to a new location."""
        pass

    @abstractmethod
    def result_location(self, node_label: str) -> str:
        """Get the result file path for a given node label."""
        pass


class MemoryResultOperations(IResultOperations):
    """Memory-based operations for results."""
    def create_location(self) -> None:
        """No location creation required for in-memory store."""
        pass

    def delete_location(self) -> None:
        """Clear the memory store."""
        self.result_io._memory_store.clear()

    def transfer_results(self, dest_location: str) -> None:
        """Not applicable for in-memory storage."""
        raise NotImplementedError("Transfer results is not supported for in-memory storage.")

    def result_location(self, node_label: str) -> str:
        """Generate a result location string."""
        return f"{self.result_io.location}/{node_label}"


class LocalResultOperations(IResultOperations):
    @classmethod
    def create_with_local_result_io(cls, location: str, serializer: ISerializeDeserialize) -> "LocalResultOperations":
        """Create LocalResultOperations with LocalResultIO constructor parameters."""
        result_io = LocalResultIO(location, serializer)
        return cls(result_io)

    @classmethod
    def create_with_temp_location(cls, serializer: ISerializeDeserialize) -> "LocalResultOperations":
        location = str(Path(tempfile.gettempdir()).resolve() / f"dag-temp-{uuid.uuid4()}")
        result_io = LocalResultIO(location, serializer)
        return cls(result_io)

    """Local file-based operations for results."""
    def create_location(self) -> None:
        """Ensure that the storage directory exists."""
        location_path = Path(self.result_io.location).resolve()
        location_path.mkdir(parents=True, exist_ok=True)

    def delete_location(self) -> None:
        """Remove the result storage directory."""
        location_path = Path(self.result_io.location).resolve()
        try:
            shutil.rmtree(location_path)
        except Exception as e:
            raise ResultIOError(f"Error deleting directory {location_path}: {e}")

    def transfer_results(self, dest_location: str) -> None:
        """Move results to a new location."""
        location_path = Path(self.result_io.location).resolve()
        dest_path = Path(dest_location).resolve()
        try:
            shutil.move(str(location_path), str(dest_path))
        except Exception as e:
            raise ResultIOError(f"Error transferring results from {location_path} to {dest_path}: {e}")

    def result_location(self, node_label: str) -> str:
        """Generate the full file path for the result."""
        location_dir = Path(self.result_io.location).resolve()
        return str(location_dir / f"{node_label}.{self.result_io.serializer.file_extension}")


class JsonSerializer(ISerializeDeserialize):
    """JSON serialization/deserialization for Result objects."""
    @property
    def file_extension(self) -> str:
        return "json"

    def serialize(self, result: Result, *args, **kwargs) -> str:
        return result.model_dump_json(*args, **kwargs)

    def deserialize(self, data: str, *args, **kwargs) -> Result:
        return Result.model_validate_json(data, *args, **kwargs)


class PickleSerializer(ISerializeDeserialize):
    """Pickle serialization/deserialization for Result objects."""
    @property
    def file_extension(self) -> str:
        return "pkl"

    def serialize(self, result: Result) -> bytes:
        return pickle.dumps(result)

    def deserialize(self, data: bytes) -> Result:
        return pickle.loads(data)
