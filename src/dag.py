import inspect
import functools
import uuid
from abc import ABC, abstractmethod
from collections import deque
from typing import Sequence, Tuple, Callable, Any, List, Dict, Optional

from decorator import decorator

from .utils import (
    _remove_duplicates,
)
from .result import ResultIO
from ._logger import create_logger
from .node import (
    Node,
    NodeRunner,
    DynamicNodeRunner,
    DependencyResultNodeRunner,
    NodeDispatcher,
    OneRunnerNodeDispatcher,
    MultiRunnerNodeDispatcher,
)


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


def _create_null_node(prefix="null-node") -> Node:
    return Node(
        label=prefix + str(uuid.uuid4()).replace('-', '')[:8]
    )


class DagTasker:
    def __init__(self, result_io: ResultIO):
        self.dag = Dag()
        self.result_io = result_io
        self.node_dispatcher = MultiRunnerNodeDispatcher()

        self._null_node = _create_null_node()
        self._setup_null_node()  # Register null node to node dispatcher

    def create_node_runner(self,
                           func: Callable[[Dict[str, Any]], Any], 
                           raise_error: bool = True,
                           get_deps: bool = False,
                           ) -> DependencyResultNodeRunner:
        return DependencyResultNodeRunner(
            func,
            self.result_io,
            raise_error=raise_error,
            get_deps=get_deps
        )

    def task(self,
             *f_args,
             name: str | None = None,
             depends_on: List[str | Node] | None = None,
             raise_error: bool = True,
             get_deps: bool = False,
             ):
        depends_on = depends_on or [self._null_node]
        if any(not isinstance(dep, (str, Node)) for dep in depends_on):
            raise TypeError("The dependencies must be a String (label) or Node.")

        def outer(func: Callable[[Dict[str, Any]], Any]):
            # Set the Node Label
            node_label = name or func.__name__

            # Check if the node label already exists in Dag
            assert node_label not in self.dag.node_labels, \
                f"The label {node_label} is already used."

            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                result_data = func(*args, **kwargs)
                return result_data

            # Extract the arguments from the function to be decorated
            f_kwgs = self._func_kwargs(func)

            # Create a node to be registered to Dag
            node = Node(label=node_label)

            # Define the direct dependencies of the node (List[Node])
            dependencies = [dep if isinstance(dep, Node) else self.dag[dep] for dep in depends_on]

            # Create a DependencyResultNodeRunner
            node_runner = self.create_node_runner(
                func,
                raise_error=raise_error,
                get_deps=get_deps
            )

            # Construct the positional arguments for the function
            # This requires the defined dependency nodes given in `depends_on`.`
            f_args_ = (dependencies,) + f_args

            # Add the node to node dispatcher
            self.node_dispatcher\
                .add_node(
                    node,
                    f_args_,
                    f_kwgs,
                    node_runner
                )

            # Add Arc to Dag
            for dependency in depends_on:
                if isinstance(dependency, str):
                    other_node = self.dag[dependency]
                    self.dag.add_arc(other_node, node)
                elif isinstance(dependency, Node):
                    # This dependency must be a source node
                    self.dag.add_arc(dependency, node)

            return wrapper
        return outer

    def _func_kwargs(self, func: Callable) -> dict:
        """Returns the keyword arguments of a function as a dictionary."""
        signature = inspect.signature(func)
        parameters = signature.parameters
        func_kwargs = {param: value.default for param, value in parameters.items() if value.default != inspect.Parameter.empty}
        return func_kwargs

    def _setup_null_node(self) -> None:
        """Registers the null node to node dispatcher."""
        def null_func(deps: List[Node]):
            return None

        node_runner = self.create_node_runner(null_func)

        self.node_dispatcher.add_node(
            self._null_node,
            ([],),
            {},
            node_runner
        )
