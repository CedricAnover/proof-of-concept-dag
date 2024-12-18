# proof-of-concept-dag

Proof-of-Concept of Concurrent DAG for Marsh (https://github.com/CedricAnover/marsh)

## Example 1

```python
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


def my_callback(node: Node, dep_results: dict[str, CustomResult], message=None) -> CustomResult:
    if message:
        print(f"[node-{node.label}] Dependency Results - {dep_results} | Message: {message}")
    else:
        print(f"[node-{node.label}] Dependency Results - {dep_results}")
    # Simulate long-running process
    time.sleep(random.randint(1, 4))
    return CustomResult(f"{node.label}-stdout", f"{node.label}-stderr")


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

res_io = LocalResultIO(TEMP_DIR, CustomResult)
async_conduit = AsyncConduit(dag, res_io)
async_conduit.start()
```

**Output:**

```
1 --> 3
2 --> 3
3 --> 4
3 --> 5
5 --> 6
4 --> 7
7 --> 8
6 --> 7

[node-1] Running.
[node-1] Dependency Results - {}
[node-2] Running.
[node-2] Dependency Results - {} | Message: Hello World
[node-2] Done.
[node-1] Done.
[node-3] Ready for execution.
[node-3] Running.
[node-3] Dependency Results - {'1': CustomResult(stdout='1-stdout', stderr='1-stderr'), '2': CustomResult(stdout='2-stdout', stderr='2-stderr')}
[node-3] Done.
[node-5] Ready for execution.
[node-4] Ready for execution.
[node-5] Running.
[node-4] Running.
[node-5] Dependency Results - {'3': CustomResult(stdout='3-stdout', stderr='3-stderr')}
[node-4] Dependency Results - {'3': CustomResult(stdout='3-stdout', stderr='3-stderr')}
[node-4] Done.
[node-5] Done.
[node-6] Ready for execution.
[node-6] Running.
[node-6] Dependency Results - {'5': CustomResult(stdout='5-stdout', stderr='5-stderr')} | Message: Some Message
[node-6] Done.
[node-7] Ready for execution.
[node-7] Running.
[node-7] Dependency Results - {'4': CustomResult(stdout='4-stdout', stderr='4-stderr'), '6': CustomResult(stdout='6-stdout', stderr='6-stderr')}
[node-7] Done.
[node-8] Ready for execution.
[node-8] Running.
[node-8] Dependency Results - {'7': CustomResult(stdout='7-stdout', stderr='7-stderr')}
[node-8] Done.
```

## Example 2

Using `node_registrator` to automatically create node from callback and register to DAG with its dependencies.

```python
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
node_1 = Node("1", my_callback)
node_2 = Node("2", my_callback, message="Hello World")


def _cb_func(node, dep_results):
    res = CustomResult(f"{node.label}-stdout", f"{node.label}-stderr")
    print(f"[node-{node.label}] {dep_results}")
    return res


# "Non-Source" nodes dependent on "Source" nodes must use a `Node` object instead of strings.
@node_registrator(dag, "3", depends_on=[node_1, node_2])
def cb_3(node, dep_results) -> CustomResult:
    return _cb_func(node, dep_results)


@node_registrator(dag, "4", depends_on=["3"])
def cb_4(node, dep_results) -> CustomResult:
    return _cb_func(node, dep_results)


@node_registrator(dag, "5", depends_on=["3"])
def cb_5(node, dep_results) -> CustomResult:
    return _cb_func(node, dep_results)


@node_registrator(dag, "6", depends_on=["5"])
def cb_6(node, dep_results) -> CustomResult:
    return _cb_func(node, dep_results)


@node_registrator(dag, "7", depends_on=["4", "6"])
def cb_7(node, dep_results) -> CustomResult:
    return _cb_func(node, dep_results)


@node_registrator(dag, "8", depends_on=["7"])
def cb_7(node, dep_results) -> CustomResult:
    return _cb_func(node, dep_results)


for src, dst in dag.arcs:
    print(f"{src} --> {dst}")
print()

res_io = LocalResultIO(TEMP_DIR, CustomResult)
async_conduit = AsyncConduit(dag, res_io)
async_conduit.start()

```

**Output:**

```
1 --> 3
2 --> 3
3 --> 4
3 --> 5
5 --> 6
4 --> 7
6 --> 7
7 --> 8

[node-1] Running.
[node-1] Dependency Results - {}
[node-2] Running.
[node-2] Dependency Results - {} | Message: Hello World
[node-1] Done.
[node-2] Done.
[node-3] Ready for execution.
[node-3] Running.
[node-3] {'1': CustomResult(stdout='1-stdout', stderr='1-stderr'), '2': CustomResult(stdout='2-stdout', stderr='2-stderr')}
[node-3] Done.
[node-5] Ready for execution.
[node-4] Ready for execution.
[node-5] Running.
[node-4] Running.
[node-4] {'3': CustomResult(stdout='3-stdout', stderr='3-stderr')}
[node-5] {'3': CustomResult(stdout='3-stdout', stderr='3-stderr')}
[node-4] Done.
[node-5] Done.
[node-6] Ready for execution.
[node-6] Running.
[node-6] {'5': CustomResult(stdout='5-stdout', stderr='5-stderr')}
[node-6] Done.
[node-7] Ready for execution.
[node-7] Running.
[node-7] {'4': CustomResult(stdout='4-stdout', stderr='4-stderr'), '6': CustomResult(stdout='6-stdout', stderr='6-stderr')}
[node-7] Done.
[node-8] Ready for execution.
[node-8] Running.
[node-8] {'7': CustomResult(stdout='7-stdout', stderr='7-stderr')}
[node-8] Done.
```
