"""Example: Using `node_registrator` decorator."""

from typing import Any

from src.conduit import AsyncConduit
from src.result import Result, LocalResultIO, MemoryResultIO
from src.dag import Dag, node_registrator
from src.node import Node


# Define a callback for Source nodes
def my_callback(node: Node, dep_results: dict[str, Result], message=None) -> Any:
    if message:
        print(f"[node-{node.label}] Dependency Results - {dep_results} | Message: {message}")
    else:
        print(f"[node-{node.label}] Dependency Results - {dep_results}")

    result_data = (f"{node.label}-stdout", f"{node.label}-stderr")
    return result_data


if __name__ == "__main__":
    # Create a Dag instance
    dag = Dag()

    # Create the Source Nodes
    node_1 = Node("1", my_callback)
    node_2 = Node("2", my_callback, message="Hello World")
    node_a = Node("A", my_callback)
    node_b = Node("B", my_callback)


    def _cb_func(node: Node, dep_results: dict[str, Result]) -> Any:
        print(f"[node-{node.label}] {dep_results}")
        return f"{node.label}-stdout"


    # "Non-Source" nodes dependent on "Source" nodes must use a `Node` object instead of strings.
    @node_registrator(dag, "3", depends_on=[node_1, node_2])
    def cb_3(node, dep_results):
        return _cb_func(node, dep_results)


    @node_registrator(dag, "4", depends_on=["3", node_a, node_b])
    def cb_4(node, dep_results):
        return _cb_func(node, dep_results)


    @node_registrator(dag, "5", depends_on=["3"])
    def cb_5(node, dep_results):
        return _cb_func(node, dep_results)


    @node_registrator(dag, "6", depends_on=["5"])
    def cb_6(node, dep_results):
        return _cb_func(node, dep_results)


    @node_registrator(dag, "7", depends_on=["4", "6"])
    def cb_7(node, dep_results):
        return _cb_func(node, dep_results)


    @node_registrator(dag, "8", depends_on=["7"])
    def cb_7(node, dep_results):
        return _cb_func(node, dep_results)


    for src, dst in dag.arcs:
        print(f"{src} --> {dst}")
    print()

    # res_io = MemoryResultIO()
    res_io = LocalResultIO()
    async_conduit = AsyncConduit(dag, res_io)
    async_conduit.start()
