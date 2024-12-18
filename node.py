from typing import Callable, Dict
from abc import ABCMeta, ABC, abstractmethod

from enums import NodeStateEnum
from result import *


class Node:
    def __init__(self,
                 label: str,
                 callback: Callable[["Node", Dict[str, Result]], Result],
                 ) -> None:
        self.label = label
        self.callback = callback
        self.state: NodeStateEnum = NodeStateEnum.IDLE

    def __str__(self) -> str:
        return self.label

    def set_state(self, new_state: NodeStateEnum) -> NodeStateEnum:
        self.state = new_state
        return self.state

    def start(self,
              dependencies: list["Node"],
              result_io: ResultIO,
              *cb_args,
              use_dependency_results: bool = True,
              **cb_kwargs
              ) -> None:
        # Set NodeStateEnum to NodeStateEnum.RUNNING
        self.set_state(NodeStateEnum.RUNNING)
        print(f"[node-{self.label}] Running.")

        # Get the Results from Dependencies
        dependency_results = dict()
        print(f"[node-{self.label}] Getting Dependency Results.")
        if use_dependency_results:
            dependency_results: dict[str, Result] = \
                {dependency.label: result_io.read_result(dependency.label) for dependency in dependencies}

        # Perform Processing and get Result object
        print(f"[node-{self.label}] Processing.")
        result = self.callback(self, dependency_results, *cb_args, **cb_kwargs)

        # Store Result object with ResultIO
        result_io.write_result(result, self.label)

        # Set NodeStateEnum to NodeStateEnum.COMPLETE
        self.set_state(NodeStateEnum.COMPLETE)
        print(f"[node-{self.label}] Done.")
