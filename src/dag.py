import inspect
import functools
import uuid
import threading
import time
from collections import deque
from typing import Sequence, Tuple, Callable, Any, List, Dict, Hashable

from .utils import (
    _remove_duplicates,
    _get_called_function_objects,
)
from .decorators import (
    _retry_func,
    _timeout_func,
)
from .exceptions import NodeError
from .result import Result
from .node import Node
from ._logger import create_logger


logger = create_logger(__name__)


class Dag:
    def __init__(self, arcs: Sequence[Tuple[Node, Node]] | None = None):
        self.arcs: Sequence[Tuple[Node, Node]] = arcs or []

    @property
    def sources(self) -> Sequence[Node]:
        return _remove_duplicates(
            [x for x, _ in self.arcs if all((y, x) not in self.arcs for y, _ in self.arcs if y != x)])

    @property
    def sinks(self) -> Sequence[Node]:
        return _remove_duplicates(
            [y for _, y in self.arcs
             if all((y, x) not in self.arcs
                    for _, x in self.arcs if x != y)]
        )

    @property
    def nodes(self) -> Sequence[Node]:
        return _remove_duplicates([x for x, _ in self.arcs] + [y for _, y in self.arcs])

    @property
    def node_labels(self) -> Sequence[str]:
        return [node.label for node in self.nodes]

    def __getitem__(self, label: str | Tuple[str, str]) -> Node | Tuple[Node, Node]:
        if isinstance(label, str):  # Returns a Node
            lst = [node for node in self.nodes if node.label == label]
            assert len(lst) == 1, "Node does not exist with the given label"
            return lst[0]
        elif isinstance(label, tuple):  # Returns Tuple[Node, Node]
            src_label = label[0]
            dst_label = label[1]
            lst = list(filter(lambda tup: tup[0] == src_label and tup[1] == dst_label, self.arcs))
            assert len(lst) == 1, "Arc does not exist with the given source & destination labels."
            return lst[0]

    def add_arc(self, src_node: Node, dst_node: Node) -> "Dag":
        if self.nodes and (src_node not in self.nodes) and (dst_node not in self.nodes):
            raise ValueError("One of the given nodes must be in the DAG.")

        temp_dag = Dag(arcs=[*self.arcs, (src_node, dst_node)])
        self.topological_sort(temp_dag)
        # self.arcs = [*self.arcs, (src_node, dst_node)]
        self.arcs = temp_dag.arcs
        return self

    @staticmethod
    def topological_sort(dag: "Dag") -> Sequence[Node]:
        sorted_nodes = deque()
        visited = []
        temp_visited = []

        def visit(node: Node):
            nonlocal sorted_nodes, visited, temp_visited
            if node in visited:
                return
            if node in temp_visited:
                raise RecursionError(f"Cycle detected.")

            temp_visited.append(node)

            # Visit all neighbors (children in the dependency graph)
            for neighbor in (neighbor for src, neighbor in dag.arcs if src == node):
                visit(neighbor)

            temp_visited.remove(node)
            visited.append(node)
            sorted_nodes.appendleft(node)

        # Process all nodes in the DAG
        for dag_node in dag.nodes:
            if dag_node not in visited:
                visit(dag_node)

        return sorted_nodes

    def all_dependencies(self, node: Node) -> Sequence[Node]:
        out_set = []
        for path in self.enumerate_paths():
            if node in path:
                idx = path.index(node)
                out_set += path[:idx]
        return out_set

    def _is_in_dag(self, node: Node) -> None:
        if node not in self.nodes:
            raise ValueError("The given node does not belong to the DAG.")

    def direct_dependencies(self, node: Node) -> Sequence[Node]:
        return [x for x, y in self.arcs if y == node]

    def neighbors(self, node: Node) -> Sequence[Node]:
        self._is_in_dag(node)
        return [y for x, y in self.arcs if x == node]

    def enumerate_paths(self) -> Sequence[Sequence[Node]]:
        out_list: list[tuple[Node]] = []

        def dfs(node: Node, path: tuple[Node]):
            # Create a new path by adding the current node to the existing immutable path
            new_path = path + (node,)

            # If the node has no neighbors (it's a sink), add the path to the result
            if len(self.neighbors(node)) == 0:
                out_list.append(new_path)
            else:
                # Recurse to each neighbor (DFS)
                for neighbor in self.neighbors(node):
                    dfs(neighbor, new_path)  # Pass the new immutable path

        # Start DFS from all source nodes
        for source_node in self.sources:
            dfs(source_node, ())  # Start with an empty tuple for the path

        return out_list

    def level(self, node: Node, path: Sequence[Node]) -> int:
        self._is_in_dag(node)
        if node not in path:
            return -1
        return path.index(node)


def _create_null_node(prefix: str = "null-node") -> Node:
    def null_func(label, deps_dict): return None
    trimmed_uid = str(uuid.uuid4()).replace('-', '')[:8]
    return Node(
        f"{prefix}-{trimmed_uid}",
        null_func,
        use_deps=False,
        raise_error=False
    )


_NULL_NODE = _create_null_node(prefix="null-node")


def node_registrator(dag: Dag,
                     label: str,
                     depends_on: list[str | Node] | None = None,
                     use_deps: bool = True,
                     raise_error: bool = True,
                     ):
    """Decorator for wrapping a custom function as a node to the given DAG."""

    depends_on = depends_on or [_NULL_NODE]

    if any(not isinstance(dep, (str, Node)) for dep in depends_on):
        raise TypeError("The dependencies must be a String (label) or Node.")

    # Check if label already used by a node in dag
    if label in dag.node_labels:
        raise ValueError("The label is already used.")

    def outer(cb_func):
        node = dag[label] if label in dag.node_labels \
            else Node(
                label,
                cb_func,
                use_deps=use_deps,
                raise_error=raise_error
            )

        for dependency in depends_on:
            if isinstance(dependency, str):
                other_node = dag[dependency]
                dag.add_arc(other_node, node)
            elif isinstance(dependency, Node):
                # This dependency must be a source node
                dag.add_arc(dependency, node)

        @functools.wraps(cb_func)
        def wrapper(*args, **kwargs):
            return cb_func(*args, **kwargs)

        return wrapper

    return outer


class DagBuilder:
    def __init__(self):
        self._dag = Dag()

    def add_node(self,
                 label: str,
                 cb_func: Callable[["Node", dict[str, Result]], Any],
                 depends_on: list[str | Node] | None = None,
                 use_deps: bool = True,
                 raise_error: bool = True,
                 *cb_args,
                 **cb_kwargs
                 ) -> "DagBuilder":

        depends_on = depends_on or [_NULL_NODE]

        node = self._dag[label] if label in self._dag.node_labels \
            else Node(
                label,
                cb_func,
                use_deps=use_deps,
                raise_error=raise_error,
                *cb_args,
                **cb_kwargs
            )

        # Register the dependency arc to the DAG
        for dependency in depends_on:
            if isinstance(dependency, str):
                other_node = self._dag[dependency]
                self._dag.add_arc(other_node, node)
            elif isinstance(dependency, Node):
                # This dependency must be a source node
                self._dag.add_arc(dependency, node)

        return self

    def reset(self) -> None:
        """Resets the internal states of the `DagBuilder`."""
        self._dag = Dag()

    def build(self) -> Dag:
        """Builds the `Dag`."""
        return self._dag


def dag_task(dag: Dag,
             lru_maxsize: int = None,
             typed: bool = False,
             raise_error=True,
             max_retries: int | None = None,
             timeout_seconds: int | None  = None,
             init_args: tuple = ()
             ):
    """
    A decorator to wrap a function as a task node in a Directed Acyclic Graph (DAG).

    This decorator registers a function as a node in the given `Dag` instance, establishing dependencies based on the functions it calls.
    The wrapped function can be executed with the provided DAG structure, which automatically resolves the dependencies between nodes.

    Args:
        dag (Dag): The DAG instance where the task node will be registered.
        lru_maxsize (int, optional): The maximum size for the LRU cache on the function's wrapper. Default is None, meaning no cache limit.
        typed (bool, optional): Arguments of different types will be cached separately.. Default is False.
        init_args (tuple, optional): The initial values for positional arguments of the function being decorated. Default is ().
            This must be given if the function to be decorated has positional arguments.

    Returns:
        Callable: The decorated function, now wrapped with task-node functionality.

    Notes:
        - The function will be associated with a unique node label generated from the function's name.
        - The decorator automatically identifies and establishes dependencies between nodes based on other functions 
            that the wrapped function calls, assuming these functions are registered as nodes in the DAG.
        - The wrapped function can accept arbitrary arguments and keyword arguments, which will be passed to
            the callback function when invoked in the context of the DAG.
        - Due to the design, all dag tasks (nodes) would get invoked regardless if it has dependencies or not. If another
            dag task tries to invoke its dependencies, it will only used the cached result if it has the same argument
            combination.
        - If the function has keyword arguments, event if another task invoked it with same argument,
            it will not use the cache invoked by the node itself.
        - For now, its best practice to use positional arguments for the function to be decorated, making
            sure that there is initial value(s) in `init_args` parameter of the dag task decorator.
        - This decorator assumes that the function to be decorated is "idempotent". In other words,
            performing the same action multiple times with the same arguments will always yield the same
            outcome, without altering the final result.

    Example:
        @dag_task(dag, raise_error=True, init_args=(2,))
        def task1(arg1):
            ...
            return ...

        @dag_task(dag, raise_error=False)
        def task2():
            ...
            value = 3
            result_1 = task1(value)
            ...
            return ...

    Raises:
        ValueError: If `init_args` is an empty tuple and the function to be decorated has positional arguments.
    """

    def outer(func: Callable[[Any], Any]):
        # Mark the function with the associated node label
        node_label_ = "node-" + func.__name__
        setattr(func, "node_label", node_label_)

        # Get all the function call in `func` and extract all node dependencies
        dep_node_labels = [fn.node_label for fn in _get_called_function_objects(func) if hasattr(fn, "node_label")]
        dep_node_labels = _remove_duplicates(dep_node_labels)
        dep_nodes = [dep_node for dep_node in dag.nodes if dep_node.label in dep_node_labels]

        # Get the signature of the function and separate args and kwargs
        signature = inspect.signature(func)
        parameters = signature.parameters
        _func_args = [param for param, value in parameters.items() if value.default == inspect.Parameter.empty]
        func_kwargs = {param: value.default for param, value in parameters.items() if value.default != inspect.Parameter.empty}

        # Throw an error if the function to be decorated has required positional arguments
        # and `dag_task` parameter `init_args` is empty.
        if _func_args and not init_args:
            err_msg = "`init_args` tuple must be given if the function to be decorated has positional arguments."
            logger.error(err_msg)
            raise ValueError(err_msg)

        if not _func_args and init_args:
            err_msg = "`init_args` must be empty if the function does not have any positional arguments."
            logger.error(err_msg)
            raise ValueError(err_msg)

        if func_kwargs:
            raise ValueError("The function to be decorated must not have any keyword arguments.")

        # Create the Wrapper
        @functools.lru_cache(maxsize=lru_maxsize, typed=typed)
        @_timeout_func(timeout_seconds)
        @_retry_func(max_retries)  # Retry calling the function if there are errors `max_retries` times.
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            result_data = func(*args, **kwargs)
            return result_data

        # Create a new callback function
        @node_registrator(
            dag,
            node_label_,
            depends_on=dep_nodes,
            use_deps=False,  # This decorator already uses LRU Cache (Memory)
            raise_error=raise_error
        )
        def cb_func(node, dep_res) -> Any:
            result_data = wrapper(*init_args, **func_kwargs)
            return result_data

        return wrapper
    return outer


class DagTasker:
    ATTR_NODE_LABEL = "node_label"

    def __init__(self):
        self._dag = Dag()

        # Storing Results after running a Conduit
        self._results_dict: Dict[str, Result] = dict()

    def _get_node_dependencies(self, func: Callable) -> List[Node]:
        """
        Gets all the function calls in the given function
        and extract all node dependencies.
        """
        dep_node_labels = [fn.node_label for fn in _get_called_function_objects(func) if hasattr(fn, self.ATTR_NODE_LABEL)]
        dep_node_labels = _remove_duplicates(dep_node_labels)
        return [dep_node for dep_node in self._dag.nodes if dep_node.label in dep_node_labels]

    def _get_func_kwargs(self, func: Callable) -> Dict[Hashable, Any]:
        """Returns the keyword arguments of a function."""
        signature = inspect.signature(func)
        parameters = signature.parameters
        return {param: value.default for param, value in parameters.items() if value.default != inspect.Parameter.empty}

    @property
    def results(self) -> Dict[str, Result]:
        return self._results_dict

    def task(self,
             *f_args,
             raise_error=True,
             lru_maxsize: int = None,
             typed: bool = False,
             ) -> Callable:

        def outer(func: Callable):
            # Throw error if function has keyword arguments (unhashable)
            f_kw = self._get_func_kwargs(func)
            if f_kw:
                raise ValueError("The function must not have any unhashable keyword arguments.")

            # Use the function's name as the node label. No prefix and suffix.
            node_label_ = func.__name__

            # Mark the function with the associated node label
            setattr(func, self.ATTR_NODE_LABEL, node_label_)

            # Get all the function call in `func` and extract all node dependencies
            dep_nodes = self._get_node_dependencies(func)

            # Create a wrapper for func
            @functools.lru_cache(maxsize=lru_maxsize, typed=typed)
            @functools.wraps(func)
            def wrapper(*args):
                result = func(*args)
                return result

            # Create a Temporary Node Callback Function and use
            # `node_registrator` to add this to the dag as node.
            @node_registrator(
                self._dag,
                node_label_,
                depends_on=dep_nodes,
                use_deps=False,
                raise_error=raise_error,
            )
            def cb_func(node, deps):
                return wrapper(*f_args)
            return wrapper
        return outer

    def retry(self, max_retries: int, sleep_for: float = 1) -> Callable:
        # Warn: This must be put on bottom of the `task` decorator
        # Example:
        # @self.dag_tasker.task()
        # @self.dag_tasker.retry(3, sleep_for=1)
        # def some_function():
        #     ...

        assert isinstance(max_retries, int) and max_retries > 0, \
            "`max_retries` must be a positive integer."

        def outer(func: Callable):
            @functools.wraps(func)
            def wrapper(*args, **kwargs) -> Any:
                assert isinstance(max_retries, int) and max_retries >= 1
                attempt = 0
                while attempt < max_retries:
                    try:
                        return func(*args, **kwargs)
                    except Exception as err:
                        attempt += 1
                        if attempt < max_retries:
                            logger.warning(f"Retrying. Current attempt {attempt} out of {max_retries}.")
                            time.sleep(sleep_for)
                        else:
                            logger.error("All attempts failed.")
                            raise NodeError(err)
            return wrapper
        return outer

    def log_result(self, func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            result_data = func(*args, **kwargs)
            logger.info(f"Result of {func.__name__}: {result_data}")
            return result_data
        return wrapper

    def start(self, conduit, *start_args, **start_kwargs) -> None:
        try:
            # Start the Conduit
            conduit.start(*start_args, **start_kwargs)

            # Collect Result
            for label in self._dag.node_labels:
                # Exclude Null Node
                if not label.startswith("null-node"):
                    self._results_dict[label] = conduit.result_io.read_result(label)
        except Exception as err:
            logger.error(err)
            raise
