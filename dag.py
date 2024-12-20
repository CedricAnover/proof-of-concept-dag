import functools
import inspect
from collections import deque
from typing import Sequence, Tuple

from node import Node


def _remove_duplicates(lst: list) -> list:
    unique_list = []
    for item in lst:
        if item not in unique_list:
            unique_list.append(item)
    return unique_list


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


def node_registrator(dag: Dag, label: str, depends_on: list[str | Node] | None = None):
    # `node_registrator` only works for non-source nodes.
    # The source nodes, has to be manually defined and referenced by other non-source nodes.
    if not depends_on:
        raise ValueError("The callback must have dependencies.")

    if any(not isinstance(dep, (str, Node)) for dep in depends_on):
        raise TypeError("The dependencies must be a String (label) or Node.")

    def outer(cb_func):
        # Construct a Node instance for the callback
        # Warn: The callback definition must have a return type hint with the result kind!!!
        sig = inspect.signature(cb_func)
        result_kind = sig.return_annotation
        if result_kind is inspect.Signature.empty:
            raise TypeError("Provide a type hint for the callback return with a result_kind.")
        node = dag[label] if label in dag.node_labels else Node(label, cb_func, result_kind)

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
