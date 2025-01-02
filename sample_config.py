from typing import Type

from src.config import Config
from src.result import (
    JsonSerializer,
    PickleSerializer,
    ResultIO,
    LocalResultIO,
    MemoryResultIO
)
from src.node import Node
from src.result import ResultIOConfig
from src.dag import Dag, DagConfig

##########################################################################################
node_1 = Node(label="node_1")
node_2 = Node(label="node_2")
node_3 = Node(label="node_3")
node_4 = Node(label="node_4")
node_5 = Node(label="node_5")
node_6 = Node(label="node_6")
node_7 = Node(label="node_7")

dag = Dag()
dag.add_arc(node_1, node_3)
dag.add_arc(node_2, node_3)
dag.add_arc(node_3, node_4)
dag.add_arc(node_3, node_5)
dag.add_arc(node_5, node_6)
dag.add_arc(node_4, node_7)
dag.add_arc(node_5, node_7)

##########################################################################################

dag_config = DagConfig(arcs=dag.arcs)
dag_config_json = dag_config.to_json()
assert isinstance(dag_config_json, str)
dag_config_revive = DagConfig.from_json(dag_config_json)
assert isinstance(dag_config_revive, DagConfig)
for src, dst in dag_config_revive.arcs:
    assert isinstance(src, Node) and isinstance(dst, Node)

dag_obj = dag_config.to_object()
assert isinstance(dag_obj, Dag)
dag_config_revive = DagConfig.from_object(dag_obj)
assert dag_config_revive == DagConfig.model_validate(dag_config_revive)

for node in dag_obj.nodes:
    assert isinstance(node, Node)
##########################################################################################

result_io = LocalResultIO("/tmp/dag", JsonSerializer())
result_io_config = ResultIOConfig.from_object(result_io)
result_io_config_json = result_io_config.to_json()
result_io_config_from_json = ResultIOConfig.from_json(result_io_config_json)
result_io_config_from_obj = ResultIOConfig.from_object(result_io)

ResultIOConfig.model_validate(result_io_config)
ResultIOConfig.model_validate(result_io_config_from_json)
ResultIOConfig.model_validate(result_io_config_from_obj)
assert result_io_config == result_io_config_from_json
assert result_io_config_from_json == result_io_config_from_obj

##########################################################################################

