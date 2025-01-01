import time

import pandas as pd
import numpy as np

from src.result import (
    MemoryResultIO,
    LocalResultOperations,
    JsonSerializer,
    PickleSerializer
)
from src.dag import (
    DagTasker
)
from src.conduit import AsyncConduit

##############################################################################

def generate_random_dataframe(rows=10, cols=5, column_names=None, value_range=(0, 100)):
    """
    Generates a random pandas DataFrame.
    
    Parameters:
        rows (int): Number of rows in the DataFrame.
        cols (int): Number of columns in the DataFrame.
        column_names (list): List of column names. If None, columns will be named "Column 1", "Column 2", etc.
        value_range (tuple): Range of values for random data (min, max).
        
    Returns:
        pandas.DataFrame: The generated random DataFrame.
    """
    # Generate random data
    data = np.random.randint(value_range[0], value_range[1], size=(rows, cols))

    # Generate column names if not provided
    if column_names is None:
        column_names = [f"Column {i+1}" for i in range(cols)]

    # Create the DataFrame
    df = pd.DataFrame(data, columns=column_names)

    return df

##############################################################################
# result_io = MemoryResultIO()

json_serializer = JsonSerializer()
pickle_serializer = PickleSerializer()
local_res_ops = LocalResultOperations.create_with_temp_location(pickle_serializer)
result_io = local_res_ops.result_io

##############################################################################
# 1 --> 2
# 1 --> 3
# 1 --> 4
# 2 --> 3
# 3 --> 4
# 1 --> 4

dag_tasker = DagTasker(result_io)

@dag_tasker.task(5, 3)
def cb_1(deps_dict: dict[str, object],
         row, col, kwg1="Hello World"
         ):
    print(f"[cb_1] Calling cb_1(kwg1='{kwg1}')...")
    print(f"[cb_1] Dependency Results: {deps_dict}")

    df = generate_random_dataframe(rows=row, cols=col)
    return df


@dag_tasker.task("Foo Bar", depends_on=["cb_1"])
def cb_2(deps_dict: dict[str, object], arg1):
    print(f"[cb_2] Calling cb_2('{arg1}')...")
    # time.sleep(2)
    print(f"[cb_2] Dependency Results: {deps_dict}")
    return arg1


@dag_tasker.task(
    3,
    depends_on=["cb_2", "cb_1"],
    get_deps=True
)
def cb_3(deps_dict: dict[str, object], arg1, kwg1=4):
    print(f"[cb_3] Calling cb_3({arg1}, kwg1={kwg1})...")

    for label, result_data in deps_dict.items():
        print(f"[cb_3] Dependency Results ({label}): {result_data}")
    return 3


@dag_tasker.task(depends_on=["cb_3", "cb_1"], get_deps=True)
def cb_4(deps_dict: dict):
    for label, result_data in deps_dict.items():
        print(f"[cb_4] Dependency Results ({label}): {result_data}")


for src, dst in dag_tasker.dag.arcs:
    print(f"{src.label} --> {dst.label}")
print()

##############################################################################
try:
    local_res_ops.create_location()
    conduit = AsyncConduit(dag_tasker.dag, dag_tasker.node_dispatcher)
    conduit.start()
except Exception as err:
    print(err)
finally:
    local_res_ops.delete_location()
