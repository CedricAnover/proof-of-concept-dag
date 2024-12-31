from typing import Callable, Dict

from .exceptions import NodeError
from .enums import NodeStateEnum
from .result import *
from ._logger import create_logger


logger = create_logger(__name__)


class Node:
    def __init__(self,
                 label: str,
                 callback: Callable[["Node", Dict[str, Result]], Any],
                 *cb_args,
                 use_deps: bool = True,
                 raise_error: bool = True,
                 **cb_kwargs
                 ) -> None:
        self.label = label
        self.callback = callback
        self.state: NodeStateEnum = NodeStateEnum.IDLE

        self._raise_error = raise_error
        self._use_deps = use_deps
        self._cb_args = cb_args
        self._cb_kwargs = cb_kwargs

    def __str__(self) -> str:
        return self.label

    def set_state(self, new_state: NodeStateEnum) -> None:
        self.state = new_state

    def start(self, dependencies: list["Node"], result_io: ResultIO) -> None:
        # Set node state to RUNNING
        self.set_state(NodeStateEnum.RUNNING)

        # Get dependency results, if specified
        dep_results = dict()
        if self._use_deps:
            dep_results: dict[str, Result] = \
                {dependency.label: result_io.read_result(dependency.label) for dependency in dependencies}

        result = None
        try:
            # Perform Processing and get result data
            result_data: Any = self.callback(self, dep_results, *self._cb_args, **self._cb_kwargs)
            result = Result(
                node_label=self.label,
                is_success=True,
                result_data=result_data
            )

            # Set node state to COMPLETE_SUCCESS
            self.set_state(NodeStateEnum.COMPLETE_SUCCESS)
        except Exception as err:
            # Raise the error if specified in constructor
            logger.error(err)
            if self._raise_error:
                raise NodeError(str(err))

            result = Result(
                node_label=self.label,
                is_success=False,
                error=str(err)
            )

            # Set node state to COMPLETE_FAIL
            self.set_state(NodeStateEnum.COMPLETE_FAIL)
        finally:
            # Store Result object with ResultIO
            if result:
                result_io.write_result(result)
