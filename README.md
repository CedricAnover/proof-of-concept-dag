# proof-of-concept-dag

Proof-of-Concept of Concurrent DAG


## TODO

- [X] Enhance: improve `Result` and `ResultIO` abstractions to allow **pickle** serialization/deserialization option.
- [X] Feature: add `IResultOperations` interface for performing local or remote file system operations on result files.
- [X] Feature: add _null_ source nodes for `node_registrator` and `DagBuilder`. Don't throw error with no dependencies.
- [] Enhance: add parameters or extra hooks to `node_registrator` & `DagBuilder` for custom logic/control other than `raise_errors` & `use_deps` such as ***retries***, ***logging***, etc.
- [] Feature: update architecture for better accomodation to _multiprocessing_ and _threading_.
- [] Feature: create common interface for node and conduit (or dag?) as both are ***startable***.
- [] Feature: update node & dag implementation where dag can be encapsulated in a node.
- [X] Feature: add decorator for building `Dag` without explicitly specifying the dependencies and just reference other DAG callbacks (use `inspect` module?). For better UX/UI.
- [] Enhance: modularize `node_registrator` & `DagBuilder`, and remove boilerplate codes.
- [] Enhance: move the **timeout** logic to node instead of conduit, if possible.
- [] ...
- [] ...
