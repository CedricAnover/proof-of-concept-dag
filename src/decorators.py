import time
import functools
import asyncio
from typing import Any, Callable

from decorator import decorator, decorate

from ._logger import create_logger
from .exceptions import NodeError, TimeoutError, NodeExecutionError


logger = create_logger(__name__)


@decorator
def run_retry(func, max_retries=3, sleep_for=1, *args, **kwgs):
    attempt = 0
    while attempt < max_retries:
        try:
            return func(*args, **kwgs)
        except Exception as err:
            attempt += 1
            if attempt < max_retries:
                time.sleep(sleep_for)
            else:
                raise NodeExecutionError(err)


@decorator
def async_timeout(func, timeout_seconds=600, *args, **kwgs):
    async def non_blocking_func(f):
        loop = asyncio.get_running_loop()
        return await asyncio.wait_for(
            loop.run_in_executor(None, functools.partial(f, *args, **kwgs)),
            timeout=timeout_seconds
        )

    try:
        loop = asyncio.get_running_loop()
        if loop.is_running():
            return loop.create_task(non_blocking_func(func))
        else:
            return asyncio.run(non_blocking_func(func))
    except RuntimeError:
        return asyncio.run(non_blocking_func(func))
    except (asyncio.TimeoutError, asyncio.CancelledError):
        raise TimeoutError(f"The function ran for more than {timeout_seconds} seconds.")
