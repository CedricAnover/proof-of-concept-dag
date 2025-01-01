from typing import Any

from decorator import decorator

from src.result import (
    Result,
    ResultIO,
    MemoryResultIO,
    LocalResultOperations,
    JsonSerializer,
    PickleSerializer
)
from src.node import (
    Node,
    AsyncNode,
    NodeRunner,
    StaticNodeRunnerMeta,
    DynamicNodeRunner,
    DependencyResultNodeRunner,
    OneRunnerNodeDispatcher,
    MultiRunnerNodeDispatcher
)
from src.dag import Dag
from src.event import AsyncObserver
from src.conduit import AsyncConduit

##############################################################################
# ====== Create the Nodes and Dag
node_1 = Node(label="node_1")
node_2 = Node(label="node_2")
node_3 = Node(label="node_3")
node_4 = Node(label="node_4")
node_5 = Node(label="node_5")
node_6 = Node(label="node_6")
node_7 = Node(label="node_7")

# Define the dependencies with Dag
dag = Dag()
dag.add_arc(node_1, node_3)
dag.add_arc(node_2, node_3)
dag.add_arc(node_3, node_4)
dag.add_arc(node_3, node_5)
dag.add_arc(node_5, node_6)
dag.add_arc(node_4, node_7)
dag.add_arc(node_5, node_7)

for src, dst in dag.arcs:
    print(f"{src.label} --> {dst.label}")
print()

# Create a ResultIO
# result_io = MemoryResultIO()
json_serializer = JsonSerializer()
pickle_serializer = PickleSerializer()
local_res_ops = LocalResultOperations.create_with_temp_location(pickle_serializer)
result_io = local_res_ops.result_io

##############################################################################
# ====== Create NodeRunners and Dispatchers

# Static Result Node Runner
def send_message(message):
    print(f"Message: {message}")
    return message

class ConcreteNodeRunner(NodeRunner, metaclass=StaticNodeRunnerMeta, func=send_message):
    pass

static_node_runner = ConcreteNodeRunner(result_io)
# static_node_runner.start(node_1, "Hello World")


# Dynamic Result Node Runner
def hello(message=None):
    print("Foo Bar")
    if message:
        print(f"Message: {message}")

dynamic_node_runner = DynamicNodeRunner(result_io)
# dynamic_node_runner.start(node_1, hello, message="Hello World")


# Dependency Result Node Runner
def node_runner_cb(result_io: ResultIO):
    node_1_res = result_io.get_result("node_1")
    print(node_1_res)
    return node_1_res.result_data

dependency_node_runner = DependencyResultNodeRunner(node_runner_cb, result_io)
# static_node_runner.start(node_1, "Hello")
# static_node_runner.start(node_2, "World")
# dependency_node_runner.start(node_3)


##############################################################################
# ====== Create Dispatcher

# one_dispatcher = (
#     OneRunnerNodeDispatcher(static_node_runner)
#     .add_node(node_1, ("[node_1] Hello World",), {})
#     .add_node(node_2, ("[node_2] Hello World",), {})
#     .add_node(node_3, ("[node_3] Hello World",), {})
#     .add_node(node_4, ("[node_4] Hello World",), {})
#     .add_node(node_5, ("[node_5] Hello World",), {})
#     .add_node(node_6, ("[node_6] Hello World",), {})
# )
# one_dispatcher("node_3")


# mult_dispatcher = (
#     MultiRunnerNodeDispatcher()
#     .add_node(node_1, ("Hello World",), {}, static_node_runner)
#     .add_node(node_2, (hello,), {"message": "node-2 message"}, dynamic_node_runner)
#     .add_node(node_3, ("Hi there",), {}, static_node_runner)
#     .add_node(node_4, (hello,), {}, dynamic_node_runner)
# )
# mult_dispatcher("node_1")


##############################################################################
# ====== Create Conduit

# Warning:
# - It may throw error if different node runners have different ResultIO.
# - ...

import time
def simulate_sleep(node: Node, sleep_for: float = 0.1):
    print(f"[{node.label}] Sleeping...")
    time.sleep(sleep_for)

def print_name(node: Node):
    print(f"[{node.label}] My name is {node.label}")
    return f"{node.label}-result"

def dep_res_cb(deps: dict[str, Any], node: Node):
    """
    Callback function to be passed to `DependencyResultNodeRunner`.
    DependencyResultNodeRunner's main requirement is the dependency
    results dictionary where the keys are the labels of the node
    dependencies, and the values are the Result objects associated
    with them. The `node` positional argument is just an auxiliary
    for referencing which node this callback is using.
    """
    print(f"[{node.label}] Dependency Results:", deps)
    return f"{node.label}-result"

# Create DependencyResultNodeRunner for referencing the results of the dependencies of a node.
dep_res_runner = DependencyResultNodeRunner(dep_res_cb, result_io)

direct_deps_of_node_6 = dag.direct_dependencies(node_6)  # Direct Dependencies of Node 6
direct_deps_of_node_7 = dag.direct_dependencies(node_7)  # Direct Dependencies of Node 7


multi_dispatcher = (
    MultiRunnerNodeDispatcher()
    .add_node(node_1, ("We are in Node 1.",), {}, static_node_runner)
    .add_node(node_2, ("We are in Node 2.",), {}, static_node_runner)
    .add_node(node_3, (print_name, node_3), {}, dynamic_node_runner)
    .add_node(node_4, (simulate_sleep, node_4), {"sleep_for": 3}, dynamic_node_runner)
    .add_node(node_5, (print_name, node_5), {}, dynamic_node_runner)
    .add_node(node_6, (direct_deps_of_node_6, node_6), {}, dep_res_runner)
    .add_node(node_7, (direct_deps_of_node_7, node_7), {}, dep_res_runner)
)

conduit = AsyncConduit(dag, multi_dispatcher)
conduit.start()
