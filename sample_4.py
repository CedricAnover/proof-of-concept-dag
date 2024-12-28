"""Example: Using `DagBuilder` to construct `Dag`."""

from pprint import pprint

from src.conduit import AsyncConduit
from src.result import Result, LocalResultIO, MemoryResultIO, JsonSerializer, PickleSerializer, LocalResultOperations
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
    # Construct a Dag with DagBuilder
    dag_builder = DagBuilder()
    dag_builder.add_node("1", my_callback)
    dag_builder.add_node("2", my_callback)
    dag_builder.add_node("3", my_callback, depends_on=["1", "2"])
    dag_builder.add_node("4", my_callback, depends_on=["3", "1"], message="Foo Bar")
    dag_builder.add_node("5", my_callback, depends_on=["3"])
    dag_builder.add_node("6", my_callback, depends_on=["5"], message="Hello World")
    dag_builder.add_node("7", my_callback, depends_on=["4", "6"])
    dag_builder.add_node("8", my_callback, depends_on=["7"])
    dag = dag_builder.build()

    for src, dst in dag.arcs:
        if src.label.startswith("null-node"):
            continue
        print(f"{src} --> {dst}")
    print()

    json_serializer = JsonSerializer()
    pickle_serializer = PickleSerializer()

    res_ops = LocalResultOperations.create_with_temp_location(json_serializer)
    # res_io = res_ops.result_io
    res_io = MemoryResultIO()

    try:
        res_ops.create_location()
        async_conduit = AsyncConduit(dag, res_io, node_timeout=100)
        async_conduit.start()
    except Exception:
        raise
    finally:
        res_ops.delete_location()
