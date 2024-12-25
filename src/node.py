from typing import Callable, Dict, Optional

from .node_state import NodeStateEnum, NodeState, IdleState, RunningState, CompleteState
from .result import Result, ResultIO


class Node:
    def __init__(self,
                 label: str,
                 callback: Callable[["Node", Dict[str, Result]], Result],
                 *cb_args,
                 use_dependency_results: bool = True,
                 state_storage_dir: Optional[str] = None,
                 **cb_kwargs
                 ) -> None:
        self.label = label
        self.callback = callback

        # Set Node Initial State
        self._state = IdleState(state_storage_dir=state_storage_dir, file_name=self.label) \
            if isinstance(state_storage_dir, str) else IdleState()

        self._use_dependency_results = use_dependency_results
        self._cb_args = cb_args
        self._cb_kwargs = cb_kwargs

    def __str__(self) -> str:
        return self.label

    @property
    def state(self) -> NodeState:
        return self._state

    def clean(self):
        # Clean State File
        self.state.delete_state_file()

    def get_state_from_file(self) -> Optional[NodeStateEnum]:
        # Try to read from the state file path if exist
        try:
            state_code = self._state.read_state()
            return NodeStateEnum(state_code) if state_code else None
        except FileNotFoundError:
            return None

    def change_state(self, complete_state: Optional[NodeStateEnum] = None) -> NodeState:
        match self._state:
            case IdleState():
                self._state = self._state.change_state()
            case RunningState():
                assert complete_state is not None, \
                    "complete_state cannot be None when transitioning to complete state"
                self._state = self._state.change_state(complete_state=complete_state)
            case CompleteState():
                pass

        return self._state

    def start(self, dependencies: list["Node"], result_io: ResultIO) -> None:
        assert isinstance(self._state, IdleState)
        self.change_state()
        assert isinstance(self._state, RunningState)

        dependency_results = dict()
        if self._use_dependency_results:
            dependency_results: dict[str, Result] = \
                {dependency.label: result_io.read_result(dependency.label) for dependency in dependencies}

        # Perform Processing and get Result object
        result = None
        try:
            result = self.callback(self, dependency_results, *self._cb_args, **self._cb_kwargs)
            self.change_state(complete_state=NodeStateEnum.COMPLETE_SUCCESS)
        except Exception as err:
            result = Result.create_fail_result(self.label, str(err))
            self.change_state(complete_state=NodeStateEnum.COMPLETE_FAIL)
        finally:
            # Store Result object with ResultIO
            result_io.write_result(result, self.label)
