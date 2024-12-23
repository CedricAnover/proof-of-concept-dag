import uuid
import tempfile
from typing import Dict
from abc import ABCMeta
from pathlib import Path

from .enums import NodeStateEnum
from .node import Node


def _state_file_path(node: Node, state_dir: str) -> Path:
    # Node State File as Text File
    state_dir_path = Path(state_dir).resolve()
    state_file_path = state_dir_path / f"{node.label}"
    return state_file_path


class DiskStateMixin(ABCMeta):
    def __new__(mcls, name, bases, attrs, /, state_dir: str, *margs, **mkwargs):
        # Define new `state` property
        @property
        def state(self) -> NodeStateEnum:
            state_file_path = _state_file_path(self, state_dir)
            node_state_value = int(state_file_path.read_text().strip())
            self._state = NodeStateEnum(node_state_value)
            return self._state

        # Define new `set_state` method
        def set_state(self, new_state: NodeStateEnum) -> NodeStateEnum:
            self._state = new_state
            state_file_path = _state_file_path(self, state_dir)
            state_file_path.write_text(f"{new_state.value}")
            return self._state

        # Modify and Update the Node attibutes before creating the new Node class
        attrs["state"] = state
        attrs["set_state"] = set_state

        new_cls = super().__new__(mcls, name, bases, attrs, *margs, **mkwargs)
        return new_cls

    def __call__(cls, *args, **kwargs):
        # Modify Node class instantiation to write the initial state (i.e. IDLE) to disk
        instance = super().__call__(*args, **kwargs)
        instance.set_state(NodeStateEnum.IDLE)
        return instance


def create_state_directory(prefix="node-states") -> Path:
    state_dir_name = f"{prefix}-{uuid.uuid4()}"
    state_dir_path = Path(tempfile.gettempdir()).resolve() / state_dir_name
    state_dir_path.mkdir(parents=True, exist_ok=False)
    return state_dir_path


class DiskStateNode(Node, metaclass=DiskStateMixin, state_dir=str(create_state_directory())):
    pass
