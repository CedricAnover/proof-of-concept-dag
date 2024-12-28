"""Example: Using `node_registrator` decorator."""
import time

from src.conduit import AsyncConduit
from src.result import JsonSerializer, PickleSerializer, LocalResultOperations
from src.dag import Dag, dag_task
from src._logger import create_logger


logger = create_logger(__name__)

COUNTER = 0


if __name__ == "__main__":
    dag = Dag()  # Create a Dag instance

    def helper_1():
        return 5

    def long_calculation(delay):
        time.sleep(delay)
        return delay

    @dag_task(dag, raise_error=False)
    def cb_1():
        return 1

    @dag_task(dag, raise_error=True, init_args=(3, "Hello World"))
    def cb_2(value, message):
        global COUNTER
        COUNTER += 1

        logger.debug(f"[cb_2] Message: {message}")
        logger.debug(f"[cb_2] Invoked with {value}")
        return long_calculation(value)

    @dag_task(dag, raise_error=False)
    def cb_3():
        res_1 = cb_1()  # 1

        logger.debug(f"[cb_3] Calling cb_2(3) ...")
        res_2 = cb_2(3, "Hello World")  # 3
        cb_2(3, "Hello World")
        cb_2(3, "Hello World")
        cb_2(3, "Hello World")
        cb_2(3, "Hello World")
        cb_2(3, "Hello World")
        cb_2(3, "Hello World")

        logger.debug(f"[cb_3] Calling cb_2(4) ...")
        res_3 = cb_2(4, "Hello World")  # 4
        cb_2(4, "Hello World")
        cb_2(4, "Hello World")
        cb_2(4, "Hello World")
        cb_2(4, "Hello World")
        cb_2(4, "Hello World")

        result_data = res_1 + res_2 + res_3
        logger.debug(f"[cb_3] Result Data: {result_data}")
        return result_data  # 8

    @dag_task(dag, raise_error=False)
    def cb_4():
        logger.debug(f"[cb_4] Invoking cb_3() ...")
        result_data = cb_3()
        logger.debug(f"[cb_4] Result Data: {result_data}")
        return None

    @dag_task(dag, raise_error=False)
    def cb_5():
        logger.debug(f"[cb_5] Invoking cb_3() ...")
        result_data = cb_3()
        cb_3()
        cb_3()
        cb_3()
        cb_3()
        logger.debug(f"[cb_5] Result Data: {result_data}")
        return None

    for src, dst in dag.arcs:
        # if src.label.startswith("null-node"): continue
        print(f"{src} --> {dst}")
    print()

    json_serializer = JsonSerializer()
    pickle_serializer = PickleSerializer()

    res_ops = LocalResultOperations.create_with_temp_location(pickle_serializer)
    res_io = res_ops.result_io

    try:
        res_ops.create_location()
        async_conduit = AsyncConduit(dag, res_io, node_timeout=10)
        async_conduit.start()
    except Exception:
        raise
    finally:
        res_ops.delete_location()

        print()
        logger.info(f"Counter: {COUNTER}")
