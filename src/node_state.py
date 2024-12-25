import tempfile
import shutil
from pathlib import Path
from enum import Enum
from abc import ABC, abstractmethod
from typing import Optional, Type


class NodeStateEnum(Enum):
    IDLE = 1
    RUNNING = 2
    COMPLETE_SUCCESS = 3
    COMPLETE_FAIL = 4


class NodeState(ABC):
    def __init__(self, state_storage_dir: Optional[str] = None, file_name: Optional[str] = None):
        if state_storage_dir is not None and file_name is None:
            raise ValueError("File name must be provided if state_storage_dir is given.")
        self._state_storage_dir = state_storage_dir
        self._file_name = file_name

    @abstractmethod
    def change_state(self, *args, **kwargs) -> Optional["NodeState"]:
        pass

    def read_state(self) -> Optional[int]:
        if not self._state_storage_dir:
            return None

        file_path = Path(self._state_storage_dir) / self._file_name
        if not file_path.exists() or not file_path.is_file():
            raise FileNotFoundError(f"State file does not exist: {file_path}")
        return int(file_path.read_text().strip())

    def write_state(self, new_state: NodeStateEnum) -> None:
        if not self._state_storage_dir:
            return

        file_path = Path(self._state_storage_dir) / self._file_name
        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_path.touch(exist_ok=True)
        file_path.write_text(str(new_state.value))

    def delete_state_file(self) -> None:
        if self._state_storage_dir:
            file_path = Path(self._state_storage_dir) / self._file_name
            file_path.unlink(missing_ok=True)


class IdleState(NodeState):
    def __init__(self, state_storage_dir=None, file_name=None):
        super().__init__(state_storage_dir, file_name)
        if state_storage_dir:
            self.write_state(NodeStateEnum.IDLE)

    def change_state(self) -> "RunningState":
        self.write_state(NodeStateEnum.RUNNING)
        return RunningState(self._state_storage_dir, self._file_name)


class RunningState(NodeState):
    def change_state(self, complete_state: NodeStateEnum) -> "CompleteState":
        if complete_state not in [NodeStateEnum.COMPLETE_SUCCESS, NodeStateEnum.COMPLETE_FAIL]:
            raise ValueError("Complete state must be either SUCCESS or FAIL.")
        self.write_state(complete_state)
        return CompleteState(self._state_storage_dir, self._file_name)


class CompleteState(NodeState):
    def change_state(self) -> None:
        pass
