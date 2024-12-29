"""Example: Using `node_registrator` decorator."""
import time
import urllib.request
from typing import Any

from src.conduit import AsyncConduit
from src.result import JsonSerializer, PickleSerializer, LocalResultOperations
from src.dag import Dag, dag_task
from src._logger import create_logger


logger = create_logger(__name__)

COUNTER_1 = 0
COUNTER_2 = 0
COUNTER_3 = 0
COUNTER_4 = 0


def web_request(url):
    try:
        # Open the URL and fetch the response
        with urllib.request.urlopen(url) as response:
            html = response.read()  # Read the response content
            html_content = html.decode('utf-8')  # Decode content to a string
            return html_content

    except urllib.error.URLError as err:
        logger.error(err)

    except Exception:
        raise


if __name__ == "__main__":
    dag = Dag()  # Create a Dag instance

    @dag_task(
        dag,
        lru_maxsize=5,
        raise_error=True,
        timeout_seconds=None,
        max_retries=2,
        init_args=("https://en.wikipedia.org/wiki/Mathematics",)
    )
    def http_request(url):
        res = len(web_request(url))
        logger.debug(f"Calling http_request({url}) ...")
        logger.debug(f"[http_request] Character Length: {res}")
        return res


    @dag_task(
        dag,
        raise_error=True,
        max_retries=3,
        timeout_seconds=2
    )
    def cb_1() -> Any:
        global COUNTER_1
        COUNTER_1 += 1
        logger.debug(f"[cb_1] Calling cb_1 ...")

        # sleep_for = 3
        # time.sleep(sleep_for)
        return 100


    @dag_task(
        dag,
        raise_error=True,
        timeout_seconds=2,
        max_retries=3
    )
    def cb_2() -> Any:
        global COUNTER_2
        COUNTER_2 += 1

        # time.sleep(3)

        logger.debug(f"[cb_2] Calling cb_2 ...")
        result_cb_1 = cb_1()  # Cached

        res = result_cb_1 + 2
        logger.debug(f"[cb_2] Result: {res}")
        # raise Exception(f"Simulated Error in cb_2.")
        return res


    @dag_task(
        dag,
        raise_error=True,
        timeout_seconds=2,
        init_args=(4,),
        max_retries=3,
    )
    def cb_3(arg1):
        global COUNTER_3
        COUNTER_3 += 1

        logger.debug(f"[cb_3] Calling cb_3({arg1}) ...")

        result_cb_1 = cb_1()  # Cached
        result_cb_2 = cb_2()  # Cached

        logger.debug(f"[cb_3] result_cb_2: {result_cb_2}")

        res = result_cb_1 + result_cb_2 + arg1
        logger.debug(f"[cb_3] Result: {res}")
        return res

    # Control LRU Max Size with `lru_maxsize`
    @dag_task(
        dag,
        lru_maxsize=5,
        raise_error=True,
        max_retries=3,
        timeout_seconds=2
    )
    def cb_4():
        global COUNTER_4
        COUNTER_4 += 1

        logger.debug(f"[cb_4] Calling cb_4 ...")

        result_cb_3 = cb_3(5)  # Not Cached, yet

        cb_3(4)  # Cached (default of cb_3)

        cb_3(5)  # Cached
        cb_3(5)  # Cached
        cb_3(5)  # Cached

        res = result_cb_3
        res = http_request("https://en.wikipedia.org/wiki/Set_theory")
        return res

    for src, dst in dag.arcs:
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
        assert COUNTER_1 == 1, f"Counter is {COUNTER_1}"
        assert COUNTER_2 == 1, f"Counter is {COUNTER_2}"
        assert COUNTER_3 == 2, f"Counter is {COUNTER_3}"
        assert COUNTER_4 == 1, f"Counter is {COUNTER_4}"
