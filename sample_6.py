"""Example: Using `DagBuilder` to construct `Dag`.
"""
import random
from pprint import pprint
from dataclasses import dataclass

from src.conduit import AsyncConduit, ThreadPoolConduit
from src.result import JsonResult, LocalResultIO
from src.dag import DagBuilder
from src.node import Node


@dataclass
class CustomResult(JsonResult):
    stdout: str
    stderr: str

    @classmethod
    def create_random(cls) -> "CustomResult":
        return cls(
            f"stdout-{random.randint(1, 500)}",
            f"stderr-{random.randint(1, 500)}"
        )


@dataclass
class LongListResult(JsonResult):
    long_list: list[float]

    @classmethod
    def create_random(cls) -> "LongListResult":
        return cls(
            [random.random() for _ in range(10)]
        )


@dataclass
class ComplexResult(JsonResult):
    output_stream_result: CustomResult
    long_list_result: LongListResult
    dict_obj: dict

    @classmethod
    def create_random(cls) -> "ComplexResult":
        return cls(
            CustomResult.create_random(),
            LongListResult.create_random(),
            {i: random.choice([random.randint(1, 10), f"{random.randint(1, 10)}"]) for i in range(1, 10 + 1)}
        )


def my_callback(node: Node, dep_results: dict[str, ComplexResult], message=None, use_print=True) -> ComplexResult:
    if use_print:
        print(f"[node-{node.label}]", end=" ")
        pprint(dep_results, indent=2)

    if message:
        print(f"[node-{node.label}] {message}")

    return ComplexResult.create_random()


if __name__ == "__main__":
    # Source Nodes
    node_1 = Node("1", my_callback, ComplexResult)
    node_2 = Node("2", my_callback, ComplexResult, message="Hello World")

    # Construct a Dag with DagBuilder
    dag_builder = DagBuilder()
    dag_builder.add_node("3", my_callback, ComplexResult, depends_on=[node_1, node_2])
    dag_builder.add_node("4", my_callback, ComplexResult, depends_on=["3", node_1], message="Foo Bar")
    dag_builder.add_node("5", my_callback, ComplexResult, depends_on=["3"])
    dag_builder.add_node("6", my_callback, ComplexResult, depends_on=["5"], message="Hello World")
    dag_builder.add_node("7", my_callback, ComplexResult, depends_on=["4", "6"])
    dag_builder.add_node("8", my_callback, ComplexResult, depends_on=["7"])
    dag = dag_builder.build()

    for src, dst in dag.arcs:
        print(f"{src} --> {dst}")
    print()

    res_io = LocalResultIO()
    conduit = AsyncConduit.create_with_clean_start(dag, res_io)
    conduit.start()
