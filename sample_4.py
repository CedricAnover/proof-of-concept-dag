"""Example: ThreadPoolConduit"""

import uuid
import tempfile
from pathlib import Path

from src.conduit import ThreadPoolConduit, AsyncConduit
from src.result import Result, LocalResultIO
from src.dag import Dag
from src.node import Node


USE_MEMORY = False
STATE_STORAGE = str(Path(tempfile.gettempdir()).resolve() / f"node-states-{uuid.uuid4()}") \
    if not USE_MEMORY else None


def my_callback(node: Node, dep_results: dict[str, Result], message=None):
    if message:
        print(f"[node-{node.label}] Dependency Results - {dep_results} | Message: {message}")
    else:
        print(f"[node-{node.label}] Dependency Results - {dep_results}")

    result_data = (f"{node.label}-stdout", f"{node.label}-stderr")
    return result_data


if __name__ == "__main__":
    node_1 = Node("1", my_callback, state_storage_dir=STATE_STORAGE)
    node_2 = Node("2", my_callback, state_storage_dir=STATE_STORAGE, message="Hello World")
    node_3 = Node("3", my_callback, state_storage_dir=STATE_STORAGE)
    node_4 = Node("4", my_callback, state_storage_dir=STATE_STORAGE)
    node_5 = Node("5", my_callback, state_storage_dir=STATE_STORAGE)
    node_6 = Node("6", my_callback, state_storage_dir=STATE_STORAGE, message="Some Message")
    node_7 = Node("7", my_callback, state_storage_dir=STATE_STORAGE)
    node_8 = Node("8", my_callback, state_storage_dir=STATE_STORAGE)

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
    print()

    res_io = LocalResultIO()
    # TODO: Fix - The process halts and does not proceed at nodes 4 & 5 when using `ThreadPoolConduit`
    # [node-5] Ready for execution.
    # [node-4] Ready for execution.
    async_conduit = AsyncConduit(dag, res_io)
    async_conduit.main_start()
