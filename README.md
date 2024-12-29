# proof-of-concept-dag

Proof-of-Concept of Concurrent DAG


## TODO

- [X] Enhance: improve `Result` and `ResultIO` abstractions to allow **pickle** serialization/deserialization option.
- [X] Feature: add `IResultOperations` interface for performing local or remote file system operations on result files.
- [X] Feature: add _null_ source nodes for `node_registrator` and `DagBuilder`. Don't throw error with no dependencies.
- [X] Enhance: add parameters or extra hooks to `node_registrator` & `DagBuilder` for custom logic/control other than `raise_errors` & `use_deps` such as ***retries***, ***logging***, etc.
- [ ] Feature: update architecture for better accomodation to _multiprocessing_ and _threading_.
- [ ] Feature: create common interface for node and conduit (or dag?) as both are ***startable***.
- [ ] Feature: update node & dag implementation where dag can be encapsulated in a node.
- [X] Feature: add decorator for building `Dag` without explicitly specifying the dependencies and just reference other DAG callbacks (use `inspect` module?). For better UX/UI.
- [ ] Enhance: modularize `node_registrator` & `DagBuilder`, and remove boilerplate codes.
- [ ] Enhance: move the **timeout** logic to node instead of conduit, if possible.
- [ ] ...
- [ ] ...

---

## Iteration 4 Experimental Features

### Removed manual source node creation

No need for manually creating source nodes with the introduction of ***Null Node***.

**Example 1:** `node_registrator` decorator

```python
from src.dag import Dag, node_registrator

dag = Dag()

@node_registrator(dag, "1")
def cb_1(node, dep_results):
    ...
    return ...

@node_registrator(dag, "2", depends_on=["1"])
def cb_2(node, dep_results):
    ...
    return ...
```

**Example 2:** DAG Builder

```python
from src.dag import DagBuilder

dag_builder = DagBuilder()
dag_builder.add_node("1", some_callback)
dag_builder.add_node("2", some_callback)
dag_builder.add_node("3", some_callback, depends_on=["1", "2"])
...
dag = dag_builder.build()
```

### Updated result module

Changes in the design of `result` module with new or updated components:

- `ISerializeDeserialize` - Decoupled from _Result_ for the purpose of Serialization/Deserialization.
    - `JsonSerializer`
    - `PickleSerializer`
- `IResultOperations` - Optional interface for performing file system operations on result files.
- `ResultIO` default constructor now requires `location (str)` and `serializer (ISerializeDeserialize)`.


**Example:**

```python
from src.conduit import AsyncConduit
from src.result import Result, LocalResultIO, MemoryResultIO, JsonSerializer, PickleSerializer, LocalResultOperations

json_serializer = JsonSerializer()
pickle_serializer = PickleSerializer()

res_ops = LocalResultOperations.create_with_temp_location(json_serializer)

res_io = res_ops.result_io
# Or
res_io = MemoryResultIO()

try:
    res_ops.create_location()

    async_conduit = AsyncConduit(dag, res_io)
    async_conduit.start()
except Exception:
    raise
finally:
    res_ops.delete_location()
```


### DAG Task Decorator

Added `@dag_task` decorator to improved UX/UI for creating nodes and DAG.

Uses an LRU cache (memory) to temporarily store the results of the default invocations of DAG tasks.

The implementation of `dag_task` uses the `node_registrator` decorator to automatically create nodes and register them to the DAG. Since it utilizes an LRU Cache, it passes `use_deps=False` to `node_registrator` to eliminate the overhead of reading the results of its dependencies. However, it will still write the results to disk (assuming that `LocalResultIO` or a remote analogue is used). Because it uses a memory-based LRU Cache, it is best practice to avoid using `MemoryResultIO` from the _result_ module. This prevents _writing_ to memory, which would otherwise introduce additional overhead.


**Example:**

```python
from src.dag import Dag, dag_task


dag = Dag()  # Create a Dag instance


@dag_task(dag)
def cb_1() -> Any:
    ...
    return ...


@dag_task(dag, raise_error=True)
def cb_2() -> Any:
    ...
    result_cb_1 = cb_1()  # Cached
    ...
    return ...


@dag_task(dag, raise_error=False, init_args=(4,))  # Needs `init_args` if there are positional arguments.
def cb_3(arg1):
    ...
    result_cb_1 = cb_1()  # Cached
    result_cb_2 = cb_2()  # Cached
    ...
    return ...


@dag_task(dag, lru_maxsize=5)  # Control LRU Max Size with `lru_maxsize`
def cb_4():
    ...
    # Calls cb_3 with different argument(s), and the result will be cached
    result_cb_3 = cb_3(5)  # Not Cached, yet
    ...
    cb_3(4)  # Cached (default of cb_3)
    ...
    cb_3(5)  # Cached
    cb_3(5)  # Cached
    cb_3(5)  # Cached
    ...
    return ...
```
