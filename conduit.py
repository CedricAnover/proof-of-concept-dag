import asyncio
from abc import ABC, abstractmethod
from typing import Sequence
from enums import NodeStateEnum
from result import ResultIO
from node import Node
from dag import Dag


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
