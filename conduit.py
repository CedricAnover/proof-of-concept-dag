import asyncio
import mmap
import struct
import multiprocessing
import signal
import math
import time
from multiprocessing import Process, cpu_count
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed, Future
from abc import ABC, abstractmethod
from typing import Sequence
from enums import NodeStateEnum
from result import ResultIO
from node import Node
from dag import Dag


class ConduitError(Exception):
    pass


class Conduit(ABC):
    def __init__(self, dag: Dag, result_io: ResultIO):
        self.dag = dag
        self.result_io = result_io

    @abstractmethod
    def start(self, *args, **kwargs) -> None:
        pass

    def get_nodes(self, node_state: NodeStateEnum) -> Sequence[Node]:
        return [node for node in self.dag.nodes if node.state == node_state]

    def are_all_nodes_complete(self) -> bool:
        return all(node.state == NodeStateEnum.COMPLETE for node in self.dag.nodes)

    def is_node_ready(self, node: Node) -> bool:
        return all(dep.state == NodeStateEnum.COMPLETE for dep in self.dag.direct_dependencies(node))


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

# TODO: Add decorator(s) for Creating and Deleting the Temporary Locations.

class AsyncConduit(Conduit):
    def __init__(self, dag: Dag, result_io: ResultIO, concurrency_limit: int = 10):
        super().__init__(dag, result_io)
        self.concurrency_limit = concurrency_limit
        self._running_nodes = set()  # Tracks currently running nodes to prevent duplicates

    async def _run_node_async(self, node: Node, dependencies: list[Node]) -> None:
        """Run a node's computation asynchronously."""
        if node in self._running_nodes:
            # Avoid duplicate execution
            return

        self._running_nodes.add(node)
        try:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, node.start, dependencies, self.result_io)
        finally:
            self._running_nodes.remove(node)

    async def _execute_ready_nodes(self, semaphore: asyncio.Semaphore) -> None:
        """Find READY nodes and execute them asynchronously within concurrency limits."""
        tasks = []
        for node in self.get_nodes(NodeStateEnum.IDLE):
            if self.is_node_ready(node):
                print(f"[node-{node.label}] Ready for execution.")
                dependencies = self.dag.direct_dependencies(node)
                tasks.append(self._schedule_node_with_semaphore(node, dependencies, semaphore))
        await asyncio.gather(*tasks)

    async def _schedule_node_with_semaphore(self, node: Node, dependencies: list[Node], semaphore: asyncio.Semaphore):
        """Wrap node execution with a semaphore for concurrency control."""
        async with semaphore:
            await self._run_node_async(node, dependencies)

    async def _main_loop(self, delay=0.1) -> None:
        """Main loop to manage DAG execution."""
        semaphore = asyncio.Semaphore(self.concurrency_limit)  # Limit concurrent tasks

        # Start source nodes (those with no dependencies)
        source_tasks = [
            asyncio.create_task(self._schedule_node_with_semaphore(node, [], semaphore))
            for node in self.dag.sources
        ]
        await asyncio.gather(*source_tasks)

        # Process the DAG until all nodes are complete
        while not self.are_all_nodes_complete():
            await self._execute_ready_nodes(semaphore)
            await asyncio.sleep(delay)

    def start(self) -> None:
        """Start the DAG execution."""
        # Delete and initialize result directory
        if hasattr(self.result_io, "delete_temp_directory"):
            self.result_io.delete_temp_directory(ignore_errors=True)

        if hasattr(self.result_io, "create_temp_directory"):
            self.result_io.create_temp_directory()

        try:
            asyncio.run(self._main_loop())
        finally:
            if hasattr(self.result_io, "delete_temp_directory"):
                self.result_io.delete_temp_directory(ignore_errors=True)


class ThreadPoolConduit(Conduit):
    def __init__(self, dag: Dag, result_io: ResultIO, *pool_args, **pool_kwargs):
        super().__init__(dag, result_io)
        self._pool_args = pool_args
        self._pool_kwargs = pool_kwargs
        self._submitted_nodes = set()  # Track nodes that have been submitted

    def _submit_node_tasks(self, executor: ThreadPoolExecutor) -> list[Future]:
        """Submit tasks for all nodes that are ready to execute."""
        futures = []

        # Start the source nodes
        for src_node in self.dag.sources:
            if src_node not in self._submitted_nodes:
                print(f"[node-{src_node.label}] Ready for execution.")
                self._submitted_nodes.add(src_node)  # Mark as submitted
                futures.append(executor.submit(src_node.start, [], self.result_io))

        # Keep submitting tasks for all ready nodes
        while not self.are_all_nodes_complete():
            for node in self.get_nodes(NodeStateEnum.IDLE):
                if node not in self._submitted_nodes and self.is_node_ready(node):
                    dependencies = self.dag.direct_dependencies(node)
                    print(f"[node-{node.label}] Ready for execution.")
                    self._submitted_nodes.add(node)  # Mark as submitted
                    futures.append(executor.submit(node.start, dependencies, self.result_io))

        return futures

    def _main_loop(self) -> None:
        with ThreadPoolExecutor(*self._pool_args, **self._pool_kwargs) as executor:
            # Submit tasks for source nodes and any other nodes that are ready
            futures = self._submit_node_tasks(executor)

            # Wait for all tasks to complete
            for future in as_completed(futures):
                try:
                    future.result()  # Wait for each task to complete and raise any exception
                except ConduitError as e:
                    error_message = f"Error occurred during task execution: {e}"
                    raise ConduitError(error_message)

    def start(self) -> None:
        """Start the DAG execution."""
        # Delete and initialize result directory
        if hasattr(self.result_io, "delete_temp_directory"):
            self.result_io.delete_temp_directory(ignore_errors=True)

        if hasattr(self.result_io, "create_temp_directory"):
            self.result_io.create_temp_directory()

        try:
            self._main_loop()
        finally:
            if hasattr(self.result_io, "delete_temp_directory"):
                self.result_io.delete_temp_directory(ignore_errors=True)
