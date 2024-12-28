"""Example: Using `node_registrator` decorator."""
import time
import tempfile
from pathlib import Path
from typing import Any

from src.conduit import AsyncConduit
from src.result import Result, LocalResultIO, MemoryResultIO, JsonSerializer, PickleSerializer, LocalResultOperations
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

    def _cb_func(node: Node, dep_results: dict[str, Result]) -> Any:
        print(f"[node-{node.label}] Dependency Results: {dep_results}")
        return f"{node.label}-stdout"


    @node_registrator(dag, "1")
    def cb_1(node, dep_results):
        return _cb_func(node, dep_results)


    @node_registrator(dag, "2")
    def cb_2(node, dep_results):
        return _cb_func(node, dep_results)


    @node_registrator(dag, "a")
    def cb_a(node, dep_results):
        return _cb_func(node, dep_results)


    @node_registrator(dag, "b")
    def cb_b(node, dep_results):
        return _cb_func(node, dep_results)


    # "Non-Source" nodes dependent on "Source" nodes must use a `Node` object instead of strings.
    @node_registrator(dag, "3", depends_on=["1", "2"])
    def cb_3(node, dep_results):
        result_1 = dep_results["1"].result_data
        result_2 = dep_results["2"].result_data
        print(f"[node-{node.label}] Result-1: {result_1}")
        print(f"[node-{node.label}] Result-2: {result_2}")


    @node_registrator(dag, "4", depends_on=["3", "a", "b"])
    def cb_4(node, dep_results):
        time.sleep(5)
        return _cb_func(node, dep_results)


    @node_registrator(dag, "5", depends_on=["3"])
    def cb_5(node, dep_results):
        return _cb_func(node, dep_results)


    @node_registrator(dag, "6", depends_on=["5"])
    def cb_6(node, dep_results):
        return _cb_func(node, dep_results)


    @node_registrator(dag, "7", depends_on=["4", "6"], raise_error=False)
    def cb_7(node, dep_results):
        raise Exception("Simulated Error")


    @node_registrator(dag, "8", depends_on=["7"])
    def cb_8(node, dep_results):
        return _cb_func(node, dep_results)


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
