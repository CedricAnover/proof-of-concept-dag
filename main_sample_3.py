"""Heterogeneous Result Kind
"""
import time
import random
from pathlib import Path
from dataclasses import dataclass

from conduit import AsyncConduit
from result import JsonResult, LocalResultIO
from dag import Dag, node_registrator
from node import Node

TEMP_DIR = str(Path(__file__).resolve().parent / ".tmp")


@dataclass
class CustomResult(JsonResult):
    stdout: str
    stderr: str


@dataclass
class CustomResultB(JsonResult):
    """This custom result kind is used for demonstrating 'heterogeneous result kind'."""
    stdout: str


# Define a callback for Source nodes
def my_callback(node: Node, dep_results: dict[str, CustomResult], message=None) -> CustomResult:
    if message:
        print(f"[node-{node.label}] Dependency Results - {dep_results} | Message: {message}")
    else:
        print(f"[node-{node.label}] Dependency Results - {dep_results}")
    # Simulate long-running process
    time.sleep(random.randint(1, 4))
    return CustomResult(f"{node.label}-stdout", f"{node.label}-stderr")


# Create a Dag instance
dag = Dag()

# Create the Source Nodes
node_1 = Node("1", my_callback, CustomResult)
node_2 = Node("2", my_callback, CustomResult, message="Hello World")


def _cb_func(node, dep_results):
    res = CustomResultB(f"{node.label}-stdout")
    # print(f"[node-{node.label}] {dep_results}")
    print(f"[node-{node.label}] {res}")
    return res


# "Non-Source" nodes dependent on "Source" nodes must use a `Node` object instead of strings.
@node_registrator(dag, "3", depends_on=[node_1, node_2])
def cb_3(node, dep_results) -> CustomResultB:
    return _cb_func(node, dep_results)


@node_registrator(dag, "4", depends_on=["3"])
def cb_4(node, dep_results) -> CustomResultB:
    return _cb_func(node, dep_results)


@node_registrator(dag, "5", depends_on=["3"])
def cb_5(node, dep_results) -> CustomResultB:
    return _cb_func(node, dep_results)


@node_registrator(dag, "6", depends_on=["5"])
def cb_6(node, dep_results) -> CustomResultB:
    return _cb_func(node, dep_results)


@node_registrator(dag, "7", depends_on=["4", "6"])
def cb_7(node, dep_results) -> CustomResultB:
    return _cb_func(node, dep_results)


@node_registrator(dag, "8", depends_on=["7"])
def cb_7(node, dep_results) -> CustomResultB:
    return _cb_func(node, dep_results)


for src, dst in dag.arcs:
    print(f"{src} --> {dst}")
print()

res_io = LocalResultIO(TEMP_DIR)
async_conduit = AsyncConduit(dag, res_io)
async_conduit.start()
