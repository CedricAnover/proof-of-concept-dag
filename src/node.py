import pickle
from typing import Any, Callable, Dict, List

from pydantic import BaseModel, Field, field_validator

from .enums import NodeStateEnum
from .result import Result, ResultIO
from ._logger import create_logger


logger = create_logger(__name__)


class NodeError(Exception):
    pass


class Node(BaseModel):
    label: str = Field(..., description="Node Label.")
    callback: Callable[[str, Dict[str, Result]], Any] = Field(..., description="Callback attached to the node.")
    # Note: We may decide to drop this feature and should be managed by conduit (or maybe dag).
    status: NodeStateEnum = Field(NodeStateEnum.IDLE, description="Node Status. Defaults to 'IDLE'.")

    @classmethod
    def create(cls, label: str, callback: Callable) -> "Node":
        if not label.strip():
            raise ValueError("Label must not be empty or whitespace.")
        if not callable(callback):
            raise ValueError("Callback must be callable.")

        return cls(label=label, callback=callback)

    def pickle_serialize(self) -> bytes:
        return pickle.dumps(self)

    @classmethod
    def pickle_deserialize(cls, data: bytes) -> "Node":
        instance = pickle.loads(data)
        if not isinstance(instance, cls):
            raise NodeError("The data bytes must be deserializable to 'Node'.")
        return instance

    @field_validator("label")
    def validate_label(cls, value: str):
        if not isinstance(value, str):
            raise NodeError("Node label must be a string.")
        if not value.strip():
            raise NodeError("Node label must not be empty or just whitespace.")
        return value

    @field_validator("status")
    def validate_status(cls, value: NodeStateEnum):
        if not isinstance(value, NodeStateEnum):
            raise NodeError("Node status must be an NodeStateEnum.")
        return value

    def get_deps(self, dependencies: List["Node"], result_io: ResultIO) -> Dict[str, Result]:
        return {dependency.label: result_io.read_result(dependency.label) for dependency in dependencies}

    def start(self,
              dependencies: List["Node"],
              result_io: ResultIO,
              raise_error: bool = True,
              use_deps: bool = False,
              *args, **kwargs
              ) -> Result:
        # Set Node Status to RUNNING
        self.status = NodeStateEnum.RUNNING

        deps_dict = dict()
        if use_deps:
            try:
                deps_dict = self.get_deps(dependencies, result_io)
            except Exception as err:
                logger.error(str(err))
                if raise_error:
                    raise NodeError(err)

        try:
            result_data = self.callback(self.label, deps_dict, *args, **kwargs)
            self.status = NodeStateEnum.COMPLETED  # Set Node Status to COMPLETED
            result = Result(
                node_label=self.label,
                is_success=True,
                result_data=result_data
            )
        except Exception as err:
            logger.error(str(err))
            self.status = NodeStateEnum.ERROR  # Set Node Status to ERROR
            result = Result(
                node_label=self.label,
                is_success=False,
                error=str(err)
            )
            if raise_error:
                raise NodeError(err)
        finally:
            result_io.write_result(result)
            return result
