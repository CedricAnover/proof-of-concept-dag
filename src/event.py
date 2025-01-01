import functools
import asyncio
from abc import ABC, abstractmethod
from typing import List, Callable, Any

from ._logger import create_logger


_logger = create_logger(__name__)


class IObserver(ABC):
    @abstractmethod
    def update(self, *arg, **kwargs) -> None:
        # Remind: The argument is typically the caller object (e.g. Node)
        # when a state changed.
        pass


class Observable(ABC):
    def __init__(self):
        self._observers: List[IObserver] = []

    def add_observer(self, observer: IObserver) -> None:
        self._observers.append(observer)

    def remove_observer(self, observer: IObserver) -> None:
        self._observers.remove(observer)

    @abstractmethod
    def notify(self) -> None:
        pass


class AsyncObserver(IObserver):
    def __init__(self, callback: Callable, executor: Any = None):
        self.callback = callback
        self.executor = executor

    async def update(self, *args, **kwargs):
        """Run the callback in separate non-blocking thread."""
        loop = asyncio.get_running_loop()
        loop.run_in_executor(self.executor, functools.partial(self.callback, *args, **kwargs))


class AsyncObservable(Observable):
    async def _async_notify(self):
        """Helper function to notify observers asynchronously."""
        for observer in self._observers:
            try:
                await observer.update(self)
            except Exception as err:
                _logger.error(err)

    def notify(self):
        """Notify all observers asynchronously."""
        try:
            # loop = asyncio.get_event_loop()  # Get the current event loop
            loop = asyncio.get_running_loop()
            if loop.is_running():
                # If an event loop is running, create a task and schedule the notification
                loop.create_task(self._async_notify())
            else:
                # If no event loop is running, use asyncio.run() to create one
                asyncio.run(self._async_notify())

        except RuntimeError:
            # If no event loop is found and we are in a non-async context, create one
            asyncio.run(self._async_notify())
