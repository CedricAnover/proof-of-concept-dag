import functools
from typing import Callable, Dict

from enums import NodeStateEnum
from result import *


def update_node_state(start_func):
    """Decorator for Node.start method to automatically update its state from RUNNING to COMPLETE."""
    @functools.wraps(start_func)
    def wrapper(self, *args, **kwargs):
        assert isinstance(self, Node)

        # Set NodeStateEnum to NodeStateEnum.RUNNING
        self.set_state(NodeStateEnum.RUNNING)

        result = start_func(self, *args, **kwargs)

        # Set NodeStateEnum to NodeStateEnum.COMPLETE
        self.set_state(NodeStateEnum.COMPLETE)
        return result
    return wrapper


class Node:
    def __init__(self,
                 label: str,
                 callback: Callable[["Node", Dict[str, Result]], Result],
                 result_kind: type[Result],
                 *cb_args,
                 use_dependency_results: bool = True,
                 **cb_kwargs
                 ) -> None:
        self.label = label
        self.callback = callback
        self.result_kind = result_kind
        self.state: NodeStateEnum = NodeStateEnum.IDLE

        self._use_dependency_results = use_dependency_results
        self._cb_args = cb_args
        self._cb_kwargs = cb_kwargs

    def __str__(self) -> str:
        return self.label

    def set_state(self, new_state: NodeStateEnum) -> NodeStateEnum:
        self.state = new_state
        return self.state

    @update_node_state
    def start(self, dependencies: list["Node"], result_io: ResultIO) -> None:
        print(f"[node-{self.label}] Running.")

        # Get the Results from Dependencies
        # Note: If `self._use_dependency_results=False`, then `dependency_results` is an empty dictionary. This is to avoid
        # the time-consuming File IO if a node only depends on the completion of its dependencies rather than requiring 
        # the dependency results for further processing.
        # Warn: If `self._use_dependency_results=False`, the callback function's dependency results parameter will be an empty dictionary
        # and cannot be used for processing as it may throw a `KeyError` due to the dictionary being empty.
        dependency_results = dict()
        if self._use_dependency_results:
            dependency_results: dict[str, Result] = \
                {dependency.label: result_io.read_result(dependency.label, dependency.result_kind) for dependency in dependencies}

        # Perform Processing and get Result object
        result = self.callback(self, dependency_results, *self._cb_args, **self._cb_kwargs)
        if not isinstance(result, self.result_kind):
            raise TypeError("The result of the node is not the same as the declared result kind.")

        # Store Result object with ResultIO
        result_io.write_result(result, self.label)

        print(f"[node-{self.label}] Done.")
