import functools
import asyncio
import threading
from typing import Any, Callable

from ._logger import create_logger
from .exceptions import NodeError


logger = create_logger(__name__)


def _retry_func(max_retries: int | None):
    # Note: If other functions depend on this failed function and
    # `raise_error=True`, then the error will propagate.
    def outer(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            if not max_retries:
                return func(*args, **kwargs)
            assert isinstance(max_retries, int) and max_retries >= 1
            attempt = 0
            while attempt < max_retries:
                try:
                    return func(*args, **kwargs)
                except Exception as err:
                    attempt += 1
                    if attempt < max_retries:
                        logger.warning(f"Retrying. Current attempt {attempt} out of {max_retries}.")
                    else:
                        logger.error("All attempts failed.")
                        raise NodeError(err)
        return wrapper
    return outer


def _async_timeout_func(timeout_seconds: int | None):
    """Asynchronous timeout decorator."""
    def decorator(func: Callable):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs) -> Any:
            if not timeout_seconds:
                return await func(*args, **kwargs)

            try:
                return await asyncio.wait_for(func(*args, **kwargs), timeout_seconds)
            except asyncio.TimeoutError:
                err_msg = f"Function '{func.__name__}' timed out after {timeout_seconds} seconds"
                logger.error(err_msg)
                raise NodeError(err_msg)
            except Exception as e:
                logger.error(f"Exception in function {func.__name__}: {e}")
                raise NodeError(e)
        return wrapper
    return decorator


def _sync_timeout_func(timeout_seconds: int | None):
    """
    Synchronous timeout decorator.

    Notes:
        - There may be slight overhead with the `_sync_timeout_func` decorator due to the
            use of threading for synchronous functions.
    """
    def decorator(func: Callable):
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            if not timeout_seconds:
                return func(*args, **kwargs)

            result = []

            def target():
                try:
                    result.append(func(*args, **kwargs))
                except Exception as e:
                    logger.error(e)
                    result.append(e)

            thread = threading.Thread(target=target, daemon=True)
            thread.start()
            thread.join(timeout_seconds)

            if thread.is_alive():
                thread._stop()  # Forcefully stop the thread
                err_msg = f"Function '{func.__name__}' timed out after {timeout_seconds} seconds"
                logger.error(err_msg)
                raise NodeError(err_msg)

            if isinstance(result[0], Exception):
                # Re-raise the exception if the function raised one
                logger.error(result[0])
                raise NodeError(result[0])
            return result[0]
        return wrapper
    return decorator


def _timeout_func(timeout_seconds: int | None):
    """General timeout decorator that selects between sync and async."""
    def decorator(func: Callable):
        if asyncio.iscoroutinefunction(func):
            return _async_timeout_func(timeout_seconds)(func)
        else:
            return _sync_timeout_func(timeout_seconds)(func)
    return decorator
