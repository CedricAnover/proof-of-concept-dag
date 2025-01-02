import pickle
from copy import deepcopy
from abc import ABCMeta, ABC, abstractmethod
from typing import Callable, Dict, AnyStr, List, Any, Optional, Tuple

from pydantic import BaseModel, Field, PrivateAttr
from decorator import decorator, decorate

from.event import IObserver, AsyncObservable
from .enums import NodeStateEnum
from .result import *
from .exceptions import NodeError, NodeExecutionError
from ._logger import create_logger


_logger = create_logger(__name__)


class NodeJsonSerializerMixin:
    @staticmethod
    def json_serialize(node: "Node", *args, **kwargs) -> str:
        # return json.dumps(node, *args, **kwargs)
        return node.model_dump_json(*args, **kwargs)

    @staticmethod
    def json_deserialize(json_str: str, *args, **kwargs) -> "Node":
        return Node.model_validate_json(json_str, *args, **kwargs)


class NodePickleSerializerMixin:
    @staticmethod
    def pickle_serialize(node: "Node", *args, **kwargs) -> bytes:
        return pickle.dumps(node, *args, **kwargs)

    @staticmethod
    def pickle_deserialize(byte_str: bytes, *args, **kwargs) -> "Node":
        return pickle.loads(byte_str, *args, **kwargs)


class Node(BaseModel, NodeJsonSerializerMixin, NodePickleSerializerMixin):
    label: str = Field(..., description="Node's label. This will also be used to act as an ID for Node.")
    status: NodeStateEnum = Field(default_factory=lambda: NodeStateEnum.IDLE, description="Status of a Node. Defaults to NodeStateEnum.IDLE.")

    def change_status(self, new_status: NodeStateEnum) -> None:
        # IDLE --> RUNNING --> {COMPLETE_SUCCESS, COMPLETE_FAIL}
        self.status = new_status
        # TODO: (Optional) Write Node States on disk.
        # TODO: (Optional) Create decorator for Pub-Sub Notifications.

    def serialize(self, format: str, *args, **kwargs) -> str | bytes:
        if format not in ["json", "pickle"]:
            raise ValueError("`format` must be json or pickle.")

        if format == "json":
            return self.json_serialize(self, *args, **kwargs)
        else:
            return self.pickle_serialize(self, *args, **kwargs)

    @classmethod
    def deserialize(cls, obj_str: AnyStr, format: str, *args, **kwargs) -> "Node":
        if format not in ["json", "pickle"]:
            raise ValueError("`format` must be json or pickle.")

        if format == "json":
            return cls.json_deserialize(obj_str, *args, **kwargs)
        else:
            return cls.pickle_deserialize(obj_str, *args, **kwargs)


class AsyncNode(Node, AsyncObservable):
    _observers: List[IObserver] = PrivateAttr(default_factory=list)

    def change_status(self, new_status: NodeStateEnum):
        """Asynchronously notify all observers when node status changed."""
        super().change_status(new_status)
        self.notify()


class NodeRunner(ABC):
    def __init__(self, result_io: ResultIO, raise_error: bool = True):
        self.result_io = result_io

        # This makes it True/False for all nodes, if NodeRunner will be instantiated once.
        self._raise_error = raise_error

        # Callbacks for Node Events
        self.on_started: Optional[Callable[[Node], None]] = None
        self.on_error: Optional[Callable[[Node, Exception], None]] = None
        self.on_success: Optional[Callable[[Node, Result], None]] = None

    @abstractmethod
    def run(self, *args, **kwargs) -> Any:
        # TODO: (Optional) Create decorators for caching the result data.
        pass

    def start(self, node: Node, *args, **kwargs) -> None:
        """Starts the Process for a Node."""
        if node.status != NodeStateEnum.IDLE:
            raise NodeExecutionError(f"{node.label} is not IDLE.")

        node.change_status(NodeStateEnum.RUNNING)

        # Call started callback
        if self.on_started and callable(self.on_started):
            self.on_started(node)

        result = None
        try:
            result_data = self.run(*args, **kwargs)
            result = Result(
                node_label=node.label,
                is_success=True,
                result_data=result_data
            )
            node.change_status(NodeStateEnum.COMPLETE_SUCCESS)

            # Call success callback
            if self.on_success and callable(self.on_success):
                self.on_success(node, result)

        except Exception as err:
            # Call error callback
            if self.on_error and callable(self.on_error):
                self.on_error(node, err)

            result = Result(
                node_label=node.label,
                is_success=False,
                error=str(err)
            )
            node.change_status(NodeStateEnum.COMPLETE_FAIL)

            # Throw NodeExecutionError, if enabled
            if self._raise_error:
                _logger.error(err)
                raise NodeExecutionError(err)
        finally:
            if result:
                self.result_io.put_result(result)


class StaticNodeRunnerMeta(ABCMeta):
    """Create new NodeRunner from a defined function."""
    def __new__(mcls, name, bases, attrs, /, func: Callable):

        # Define the run method that uses the function
        def run(self, *args, **kwargs):
            return func(*args, **kwargs)

        # Add the run method to the class's attributes
        attrs['run'] = run

        new_cls = super().__new__(mcls, name, bases, attrs)
        return new_cls


class DynamicNodeRunner(NodeRunner):
    # Define the run method that requires another function to be
    # called along with its arguments.
    def run(self, func, *args, **kwargs) -> Any:
        return func(*args, **kwargs)


class DependencyResultNodeRunner(NodeRunner):
    def __init__(self,
                 callback: Callable[[Dict[str, Any]], Any],
                 result_io,
                 raise_error = True,
                 get_deps: bool = True,
                 ):
        # The callback's signature is:
        # (Dict[str -> Any], ...) => Any
        # where the keys are the labels of the node dependencies
        # and the values are their result data (not the Result object).
        super().__init__(result_io, raise_error)
        self.callback = callback
        self.get_deps = get_deps

    def run(self, dependencies: List[Node], *args, **kwargs) -> Any:
        dependency_results: Dict[str, Any] = {}
        if self.get_deps:
            # If `get_deps=True`, DependencyResultNodeRunner will use the ResultIO
            # to get all the result data of a node's dependencies. If set to False,
            # this will minimize the overhead of reading the result data (e.g. from
            # JSON or Pickle file).
            dependency_results = {
                dep.label: self.result_io.get_result(dep.label).result_data
                for dep in dependencies
            }

        return self.callback(dependency_results, *args, **kwargs)


class NodeDispatcher:
    """
    Base class for Node dispatchers.

    UX/UI:
        ```python
        node_dispatcher = NodeDispatcher(...)
        node_dispatcher.add_node(...)
        node_dispatcher.add_node(...)
        ...
        # Or
        node_dispatcher = (
            NodeDispatcher(...)
            .add_node(...)
            .add_node(...)
            ...
        )

        # Get the tuple of a node dispatcher by node's label
        tup = node_dispatcher["node_label"]

        # Start the NodeRunner by calling the node dispatcher
        # with the node's label
        node_dispatcher("node_label")
        ```
    """

    def __init__(self):
        self._node_data: List[Tuple[Node, tuple, dict] | Tuple[Node, tuple, dict, NodeRunner]] = []

    @property
    def nodes(self) -> List[Node]:
        """Returns all nodes in the dispatcher."""
        return [node for node, _, _ in self._node_data] if len(self._node_data[0]) == 3 else [node for node, _, _, _ in self._node_data]

    def add_node(self, node: Node, args: tuple, kwargs: dict, node_runner: Optional[NodeRunner] = None) -> "NodeDispatcher":
        """Add a node to the dispatcher."""
        if node_runner:
            self._node_data.append((node, args, kwargs, node_runner))
        else:
            self._node_data.append((node, args, kwargs))
        return self

    def __getitem__(self, node_label: str) -> Tuple[Node, tuple, dict] | Tuple[Node, tuple, dict, NodeRunner]:
        """Get a node by label."""
        filtered = [
            data for data in self._node_data if data[0].label == node_label
        ]
        if len(filtered) != 1:
            err_msg = f"{node_label} does not exist or it has duplicates."
            raise KeyError(err_msg)
        return filtered[0]

    def __call__(self, node_label: str) -> None:
        """Start the NodeRunner for the node."""
        node_data = self[node_label]
        if len(node_data) == 3:
            node, args, kwargs = node_data
            self._start_node_runner(node, args, kwargs)
        else:
            node, args, kwargs, node_runner = node_data
            node_runner.start(node, *args, **kwargs)

    def _start_node_runner(self, node: Node, args: tuple, kwargs: dict) -> None:
        """Helper method to start a NodeRunner."""
        raise NotImplementedError("This method should be implemented by subclass.")


class OneRunnerNodeDispatcher(NodeDispatcher):
    """1 NodeRunner for all Nodes."""

    def __init__(self, node_runner: NodeRunner):
        super().__init__()
        self.node_runner = node_runner

    def _start_node_runner(self, node: Node, args: tuple, kwargs: dict) -> None:
        """Start the single NodeRunner for the node."""
        self.node_runner.start(node, *args, **kwargs)


class MultiRunnerNodeDispatcher(NodeDispatcher):
    """1 NodeRunner per Node."""

    def __init__(self):
        super().__init__()

    def add_node(self, node: Node, args: tuple, kwargs: dict, node_runner: NodeRunner) -> "MultiRunnerNodeDispatcher":
        """Override to add node with a NodeRunner."""
        return super().add_node(node, args, kwargs, node_runner)

    def _start_node_runner(self, node: Node, args: tuple, kwargs: dict) -> None:
        """Start the appropriate NodeRunner for the node."""
        node_data = self[node.label]
        _, args, kwargs, node_runner = node_data
        node_runner.start(node, *args, **kwargs)


class NodeDispatcherWrapper:
    """Wrapper to extend NodeDispatcher's __call__ with override functionality."""

    def __init__(self, dispatcher: NodeDispatcher):
        """
        Initialize the wrapper with a NodeDispatcher instance.

        Args:
            dispatcher (NodeDispatcher): The dispatcher instance to wrap.
        """
        self._dispatcher = dispatcher

    def __getattr__(self, name):
        """Delegate attribute access to the wrapped dispatcher."""
        return getattr(self._dispatcher, name)

    def __call__(self, node_label: str, *override_args, **override_kwargs):
        """
        Override the __call__ method to allow overriding args and kwargs.

        Args:
            node_label (str): The label of the node to call.
            *override_args: Positional arguments to override.
            **override_kwargs: Keyword arguments to override.
        """
        node_data = self._dispatcher[node_label]
        if len(node_data) == 3:
            node, args, kwargs = node_data
            # Override args and kwargs if provided
            args = override_args if override_args else args
            kwargs = {**kwargs, **override_kwargs}
            self._dispatcher._start_node_runner(node, args, kwargs)
        else:
            node, args, kwargs, node_runner = node_data
            # Override args and kwargs if provided
            args = override_args if override_args else args
            kwargs = {**kwargs, **override_kwargs}
            node_runner.start(node, *args, **kwargs)
