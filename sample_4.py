"""Example: Using `DagBuilder` to construct `Dag`."""

from pprint import pprint

from src.conduit import AsyncConduit, ThreadConduit, ThreadPoolConduit
from src.result import Result, LocalResultIO, MemoryResultIO
from src.dag import DagBuilder
from src.node import Node


def my_callback(node: Node, dep_results: dict[str, Result], message=None, use_print=True):
    if use_print:
        print(f"[node-{node.label}]", end=" ")
        pprint(dep_results, indent=2)

    if message:
        print(f"[node-{node.label}] {message}")

    return f"node-{node.label}-result"


if __name__ == "__main__":
    # Source Nodes
    node_1 = Node("1", my_callback)
    node_2 = Node("2", my_callback, message="Hello World")

    # Construct a Dag with DagBuilder
    dag_builder = DagBuilder()
    dag_builder.add_node("3", my_callback, depends_on=[node_1, node_2])
    dag_builder.add_node("4", my_callback, depends_on=["3", node_1], message="Foo Bar")
    dag_builder.add_node("5", my_callback, depends_on=["3"])
    dag_builder.add_node("6", my_callback, depends_on=["5"], message="Hello World")
    dag_builder.add_node("7", my_callback, depends_on=["4", "6"])
    dag_builder.add_node("8", my_callback, depends_on=["7"])
    dag = dag_builder.build()

    for src, dst in dag.arcs:
        print(f"{src} --> {dst}")
    print()

    conduit = ThreadPoolConduit(dag)
    # conduit = ThreadConduit(dag)
    conduit.start()

    # res_io = LocalResultIO()
    # res_io = MemoryResultIO()
    # conduit = AsyncConduit(dag, res_io)
    # conduit.start()
