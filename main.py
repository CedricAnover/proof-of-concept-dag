import time
import random
from pathlib import Path
from dataclasses import dataclass

from conduit import AsyncConduit
from result import JsonResult, LocalResultIO
from dag import Dag
from node import Node

TEMP_DIR = str(Path(__file__).resolve().parent / ".tmp")


@dataclass
class CustomResult(JsonResult):
    stdout: str
    stderr: str


def my_callback(node_: Node, dep_results_dict: dict[str, CustomResult], message=None) -> CustomResult:
    if message:
        print(f"[node-{node_.label}] Dependency Results - {dep_results_dict} | Message: {message}")
    else:
        print(f"[node-{node_.label}] Dependency Results - {dep_results_dict}")
    # Simulate long-running process
    time.sleep(random.randint(1, 4))
    return CustomResult(f"{node_.label}-stdout", f"{node_.label}-stderr")


node_1 = Node("1", my_callback)
node_2 = Node("2", my_callback, message="Hello World")
node_3 = Node("3", my_callback)
node_4 = Node("4", my_callback)
node_5 = Node("5", my_callback)
node_6 = Node("6", my_callback, message="Some Message")
node_7 = Node("7", my_callback)
node_8 = Node("8", my_callback)

dag = Dag()
dag.add_arc(node_1, node_3)
dag.add_arc(node_2, node_3)
dag.add_arc(node_3, node_4)
dag.add_arc(node_3, node_5)
dag.add_arc(node_5, node_6)
dag.add_arc(node_4, node_7)
dag.add_arc(node_7, node_8)
dag.add_arc(node_6, node_7)

for src, dst in dag.arcs:
    print(f"{src} --> {dst}")

res_io = LocalResultIO(TEMP_DIR, CustomResult)
async_conduit = AsyncConduit(dag, res_io)
async_conduit.start()
