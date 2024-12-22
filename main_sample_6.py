"""Example: Using `DagBuilder` to construct `Dag`.
"""
from dataclasses import dataclass

from conduit import AsyncConduit
from result import JsonResult, LocalResultIO
from dag import DagBuilder
from node import Node


@dataclass
class CustomResult(JsonResult):
    stdout: str
    stderr: str


def my_callback(node: Node, dep_results: dict[str, CustomResult], message=None) -> CustomResult:
    if message:
        print(f"[node-{node.label}] Dependency Results - {dep_results} | Message: {message}")
    else:
        print(f"[node-{node.label}] Dependency Results - {dep_results}")
    return CustomResult(f"{node.label}-stdout", f"{node.label}-stderr")


# Source Nodes
node_1 = Node("1", my_callback, CustomResult)
node_2 = Node("2", my_callback, CustomResult, message="Hello World")

# Construct a Dag with DagBuilder
dag_builder = DagBuilder()
dag_builder.add_node("3", my_callback, CustomResult, depends_on=[node_1, node_2])
dag_builder.add_node("4", my_callback, CustomResult, depends_on=["3", node_1], message="Foo Bar")
dag_builder.add_node("5", my_callback, CustomResult, depends_on=["3"])
dag_builder.add_node("6", my_callback, CustomResult, depends_on=["5"], message="Hello World")
dag_builder.add_node("7", my_callback, CustomResult, depends_on=["4", "6"])
dag_builder.add_node("8", my_callback, CustomResult, depends_on=["7"])
dag = dag_builder.build()

for src, dst in dag.arcs:
    print(f"{src} --> {dst}")
print()

res_io = LocalResultIO()
async_conduit = AsyncConduit.create_with_clean_start(dag, res_io)
async_conduit.start()
