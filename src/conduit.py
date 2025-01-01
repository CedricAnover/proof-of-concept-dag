import asyncio
import functools
from multiprocessing import Process, cpu_count
from abc import ABC, abstractmethod
from typing import Sequence

from exceptions import ConduitError

from .enums import NodeStateEnum
from .node import Node, NodeDispatcher
from .dag import Dag
from ._logger import create_logger


logger = create_logger(__name__)


class Conduit(ABC):
    def __init__(self, dag: Dag, node_dispatcher: NodeDispatcher):
        self.dag = dag
        self.node_dispatcher = node_dispatcher

    def get_nodes(self, node_state: NodeStateEnum) -> Sequence[Node]:
        return [node for node in self.dag.nodes if node.status == node_state]

    def are_all_nodes_complete(self) -> bool:
        return all(node.status in [NodeStateEnum.COMPLETE_SUCCESS, NodeStateEnum.COMPLETE_FAIL] for node in self.dag.nodes)

    def is_node_ready(self, node: Node) -> bool:
        return all(dep.status in [NodeStateEnum.COMPLETE_SUCCESS, NodeStateEnum.COMPLETE_FAIL] for dep in self.dag.direct_dependencies(node))

    @abstractmethod
    def start(self, *args, **kwargs) -> None:
        pass


class ParallelConduits:
    """Run multiple conduits in parallel."""
    def __init__(self, name: str, max_processors: int = 4):
        if max_processors > cpu_count():
            raise ValueError("max_processors exceeds the number of cpu in this machine.")

        self.name = name

        self._processors: list[Process] = []
        self._max_processors = max_processors

    @property
    def num_processors(self) -> int:
        return len(self._processors)

    def add_conduit(self, conduit: Conduit, *start_args, **start_kw) -> None:
        if self.num_processors >= self._max_processors:
            raise ValueError("Adding a new conduit would exceed the maximum number of worker processors.")

        processor = Process(
            target=conduit.start,
            name=f"{self.name}-{self.num_processors + 1}",
            args=start_args,
            kwargs=start_kw
        )
        self._processors.append(processor)

    def start(self) -> None:
        for proc in self._processors:
            print(f"Starting {proc.name} ...")
            proc.start()
        for proc in self._processors:
            print(f"{proc.name} Completed.")
            proc.join()


class AsyncConduit(Conduit):
    def __init__(self, dag: Dag, node_dispatcher: NodeDispatcher, concurrency_limit: int = 10, node_timeout: float = 300):
        super().__init__(dag, node_dispatcher)
        self.concurrency_limit = concurrency_limit
        self.node_timeout = node_timeout  # Timeout for node execution (in seconds)
        self._node_tasks = {}  # Track tasks for all nodes

    async def _async_node_start(self, node: Node) -> None:
        """Run a node's computation asynchronously in a thread pool with timeout."""
        loop = asyncio.get_running_loop()

        try:
            # Run the node in the thread pool with a timeout
            await asyncio.wait_for(
                loop.run_in_executor(
                    None,  # Use the default ThreadPoolExecutor
                    functools.partial(self.node_dispatcher, node.label)
                ),
                timeout=self.node_timeout  # Apply timeout to each node's execution
            )

        except asyncio.TimeoutError:
            err_msg = f"Execution timed out for node {node.label} after {self.node_timeout} seconds."
            logger.error(err_msg)
            raise ConduitError(err_msg)

    async def _run_node(self, node: Node, semaphore: asyncio.Semaphore):
        """Execute a node, ensuring its dependencies are complete."""
        # Wait for all dependency tasks to complete
        dependency_tasks = [
            self._node_tasks[dep.label] for dep in self.dag.direct_dependencies(node)
        ]

        # Wait for all dependencies to finish
        if dependency_tasks:
            await asyncio.gather(*dependency_tasks)

        # Run the node itself with semaphore to control concurrency
        async with semaphore:
            await self._async_node_start(node)

    async def _main_loop(self) -> None:
        """Main Loop."""
        semaphore = asyncio.Semaphore(self.concurrency_limit)  # Control concurrent tasks

        # Create tasks for all nodes upfront
        for node in self.dag.nodes:
            self._node_tasks[node.label] = asyncio.create_task(self._run_node(node, semaphore))

        await asyncio.gather(*self._node_tasks.values())  # Wait for all tasks to complete

    def start(self) -> None:
        """Start the DAG."""
        asyncio.run(self._main_loop())
