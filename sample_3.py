"""Example: Using `ParallelConduits` to run multiple conduits in parallel."""

import random

from src.conduit import AsyncConduit, ParallelConduits, ThreadConduit, ThreadPoolConduit
from src.result import Result, LocalResultIO, MemoryResultIO
from src.dag import Dag
from src.node import Node


def my_callback(node: Node, dep_results: dict[str, Result], message=None):
    if message:
        print(f"[node-{node.label}] Dependency Results - {dep_results} | Message: {message}")
    else:
        print(f"[node-{node.label}] Dependency Results - {dep_results}")
    
    result_data = (f"{node.label}-stdout", f"{node.label}-stderr")
    return result_data


def create_dag() -> Dag:
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
    print()
    return dag


def random_conduit(dag: Dag) -> AsyncConduit | ThreadConduit | ThreadPoolConduit:
    def random_result_io() -> LocalResultIO | MemoryResultIO:
        rand_result_io = random.choice([LocalResultIO, MemoryResultIO])
        return rand_result_io()

    rand_conduit_cls = random.choice([AsyncConduit, ThreadConduit, ThreadPoolConduit])

    if rand_conduit_cls is AsyncConduit:
        rand_res_io = random_result_io()
        return rand_conduit_cls(dag, rand_res_io)

    return rand_conduit_cls(dag)


def main():
    max_processors = 4

    parallel_conduits = ParallelConduits("my-parallel-conduits", max_processors=max_processors)
    for _ in range(max_processors):
        dag = create_dag()

        # conduit = ThreadConduit(dag)
        conduit = ThreadPoolConduit(dag)
        conduit.start()
        parallel_conduits.add_conduit(conduit)

        # res_io = LocalResultIO()
        # async_conduit = AsyncConduit(dag, res_io)
        # parallel_conduits.add_conduit(async_conduit)

        # rand_conduit = random_conduit(dag)
        # parallel_conduits.add_conduit(rand_conduit)

    parallel_conduits.start()


if __name__ == "__main__":
    main()
