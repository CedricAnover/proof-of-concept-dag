import tempfile
from pathlib import Path

from src.conduit import AsyncConduit
from src.result import Result, LocalResultIO
from src.dag import Dag
from src.node import Node


USE_MEMORY = False
STATE_STORAGE = str(Path(tempfile.gettempdir()).resolve() / "node-states") \
    if not USE_MEMORY else None


def my_callback(node: Node, dep_results: dict[str, Result], message=None) -> Result:
    if message:
        print(f"[node-{node.label}] Dependency Results - {dep_results} | Message: {message}")
    else:
        print(f"[node-{node.label}] Dependency Results - {dep_results}")

    for dep_label, dep_result in dep_results.items():
        if not dep_result.is_success:
            print(f"Dependency {dep_label} has failed")

    return Result(
        node_label=node.label,
        is_success=True,
        data=f"node-{node.label}-data",
    )


def fail_callback(node: Node, dep_results: dict[str, Result], message=None) -> Result:
    raise Exception("Simulated Error.")


node_1 = Node("1", my_callback, state_storage_dir=STATE_STORAGE)
node_2 = Node("2", my_callback, state_storage_dir=STATE_STORAGE, message="Hello World")
node_3 = Node("3", fail_callback, state_storage_dir=STATE_STORAGE)
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
async_conduit = AsyncConduit(dag, res_io)
async_conduit.start()
