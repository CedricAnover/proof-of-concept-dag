import time
import unittest
from unittest.mock import MagicMock, patch

from src._logger import create_logger
from src.dag import DagTasker, Dag
from src.node import Node
from src.exceptions import NodeError, ResultDataError
from src.conduit import AsyncConduit
from src.result import JsonSerializer, PickleSerializer, LocalResultOperations


_logger = create_logger(__name__)


class TestDagTasker(unittest.TestCase):
    def setUp(self):
        self.dag_tasker = DagTasker()
        self.call_counter = 0

    @staticmethod
    def printer(dag_tasker: DagTasker):
        _logger.info(f"Nodes: {dag_tasker._dag.node_labels}")
        for src, dst in dag_tasker._dag.arcs:
            _logger.info(f"{src.label} --> {dst.label}")

    def test_instantiation(self):
        self.assertIsInstance(self.dag_tasker, DagTasker)
        self.assertIsInstance(self.dag_tasker._dag, Dag)
        self.assertEqual(len(self.dag_tasker.results), 0)
        self.assertEqual(self.dag_tasker.ATTR_NODE_LABEL, "node_label")

    def test_add_task_without_dependencies(self):
        """Test adding a task without dependencies."""
        @self.dag_tasker.task()
        def task1():
            return "task1-result"

        result_data = task1()

        self.assertTrue(hasattr(task1, '__wrapped__'))  # Function is Wrapped
        self.assertTrue(hasattr(task1, "node_label"))  # Function has `node_label` attribute
        self.assertEqual(task1.node_label, "task1")  # node_label.node_label is equal to function name
        self.assertEqual(result_data, "task1-result")

    def test_add_task_with_one_dependency(self):
        @self.dag_tasker.task()
        def task1():
            return "task1-result"

        @self.dag_tasker.task()
        def task2():
            task1_result = task1()
            return task1_result

        result_data_task1 = task1()
        result_data_task2 = task2()
        
        self.assertEqual(result_data_task1, result_data_task2)

        self.assertEqual(task1.node_label, "task1")
        self.assertEqual(task2.node_label, "task2")

        for fn in [task1, task2]:
            self.assertTrue(hasattr(fn, '__wrapped__'))
            self.assertTrue(hasattr(fn, "node_label"))

        self.assertEqual(len(self.dag_tasker._dag.nodes), 2 + 1)  # `1` for the Null Node

        for src, dst in self.dag_tasker._dag.arcs:
            if src.label.startswith("null-node"):  # i.e. dst must be the task1
                self.assertEqual(dst.label, "task1")

    def test_add_task_with_multiple_dependencies(self):
        @self.dag_tasker.task()
        def task1():
            return "task1-result"

        @self.dag_tasker.task()
        def task2():
            return "task2-result"
        
        @self.dag_tasker.task()
        def task3():
            task1_result = task1()
            task2_result = task2()
            return task1_result + " and " + task2_result

        # Arrang and Act for Task 3 Expected Result
        expected_result = "task1-result and task2-result"
        task3_result = task3()

        dag = self.dag_tasker._dag
        task1_node = self.dag_tasker._dag["task1"]
        task2_node = self.dag_tasker._dag["task2"]
        task3_node = self.dag_tasker._dag["task3"]

        self.assertEqual(task3_result, expected_result)

        self.assertEqual(task1.node_label, "task1")
        self.assertEqual(task2.node_label, "task2")
        self.assertEqual(task3.node_label, "task3")

        for fn in [task1, task2, task3]:
            self.assertTrue(hasattr(fn, '__wrapped__'))
            self.assertTrue(hasattr(fn, "node_label"))

        self.assertEqual(len(self.dag_tasker._dag.nodes), 3 + 1)

        # Null Node is the same for both task1 and task2
        self.assertEqual(
            dag.direct_dependencies(task1_node),
            dag.direct_dependencies(task2_node)
        )

        self.assertEqual(len(dag.direct_dependencies(task1_node)), 1)  # The null node
        self.assertEqual(len(dag.direct_dependencies(task2_node)), 1)  # The null node
        self.assertEqual(len(dag.direct_dependencies(task3_node)), 2)  # task1 and task2

        task3_dep_nodes = dag.direct_dependencies(task3_node)
        self.assertIn(task1_node, task3_dep_nodes)
        self.assertIn(task2_node, task3_dep_nodes)

    def test_add_task_with_same_dependency(self):
        @self.dag_tasker.task()
        def task1():
            return "task1-result"

        @self.dag_tasker.task()
        def task2():
            return task1()

        @self.dag_tasker.task()
        def task3():
            return task1()

        dag = self.dag_tasker._dag
        task1_node = self.dag_tasker._dag["task1"]
        task2_node = self.dag_tasker._dag["task2"]
        task3_node = self.dag_tasker._dag["task3"]

        self.assertEqual(len(dag.direct_dependencies(task1_node)), 1)  # Null Node
        self.assertEqual(dag.direct_dependencies(task2_node), [task1_node])
        self.assertEqual(dag.direct_dependencies(task3_node), [task1_node])

        task1_result = task1()
        task2_result = task2()
        task3_result = task3()
        self.assertEqual(task2_result, task3_result)
        self.assertEqual(task2_result, task1_result)
        self.assertEqual(task3_result, task1_result)

    def test_dependency_calls_with_no_arguments(self):
        # Dependencies
        # ============
        # null --> 1
        # 1 --> 2
        # 1 --> 3

        @self.dag_tasker.task()
        def task1():
            self.call_counter += 1  # Track the number of calls
            return "task1-result"

        @self.dag_tasker.task()
        def task2():
            res = task1()  # Cached
            return "task2-result"

        @self.dag_tasker.task()
        def task3():
            res = task1()  # Cached
            return "task3-result"

        # Act by calling both tasks 2 and 3, multiple times
        task2()
        task2()
        task2()

        task3()
        task3()
        task3()

        # The result of task1 is cached for no arguments
        self.assertEqual(self.call_counter, 1)

    def test_dependency_calls_with_arguments(self):
        @self.dag_tasker.task("task1-arg")
        def task1(value):
            self.call_counter += 1  # Track the number of calls
            return value

        @self.dag_tasker.task()
        def task2():
            res = task1("task1-arg")  # Cached
            return res

        @self.dag_tasker.task()
        def task3():
            res = task1("task3-arg")  # Not Cached, yet
            return res

        # Act by calling both tasks 2 and 3
        task2()
        task2()
        task2()

        task3()
        task3()
        task3()

        self.assertEqual(self.call_counter, 2)


class TestDagTaskerWithConduit(unittest.TestCase):
    def setUp(self):
        # Dependencies
        # ============
        # null --> {1, 2}
        # {1, 2} --> 3
        # 3 --> {5, 6}
        # 6 --> 7
        # 2 --> 7

        self.counter = {f"task{i}": 0 for i in range(1, 7 + 1)}

        self.dag_tasker = DagTasker()

        @self.dag_tasker.task()
        def task1():
            self.counter["task1"] += 1
            return "task1-result"

        @self.dag_tasker.task()
        def task2():
            self.counter["task2"] += 1
            return "task2-result"

        @self.dag_tasker.task(True)
        def task3(value):
            self.counter["task3"] += 1
            # return "task3-result"
            return [task1(), task2()]  # Cached

        @self.dag_tasker.task()
        def task4():
            self.counter["task4"] += 1
            return "task4-result"

        @self.dag_tasker.task()
        def task5():
            self.counter["task5"] += 1
            return [*task3(True)]  # Cached

        @self.dag_tasker.task()
        def task6():
            self.counter["task6"] += 1
            return [*task3(False)]  # Not Cached

        @self.dag_tasker.task()
        def task7():
            self.counter["task7"] += 1
            return [task2(), *task6()]

        self._start_conduit()
        self.addCleanup(self.cleanup)

    def cleanup(self):
        try:
            self.res_ops.delete_location()
        except Exception as err:
            _logger.error(err)

    def _start_conduit(self):
        json_serializer = JsonSerializer()
        pickle_serializer = PickleSerializer()
        self.res_ops = LocalResultOperations.create_with_temp_location(pickle_serializer)
        self.res_io = self.res_ops.result_io
        self.conduit = AsyncConduit(self.dag_tasker._dag, self.res_io)
        self.dag_tasker.start(self.conduit)

    def test_dependency_call_counts(self):
        self.assertEqual(self.counter["task1"], 1)
        self.assertEqual(self.counter["task2"], 1)
        self.assertEqual(self.counter["task3"], 2)
        self.assertEqual(self.counter["task4"], 1)
        self.assertEqual(self.counter["task5"], 1)
        self.assertEqual(self.counter["task6"], 1)
        self.assertEqual(self.counter["task7"], 1)

    def test_dag_tasker_results(self):
        results = self.dag_tasker.results
        self.assertIsInstance(results, dict)

        self.assertEqual(len(results), 7)

        self.assertEqual(results["task1"].result_data, "task1-result")
        self.assertEqual(results["task2"].result_data, "task2-result")
        self.assertEqual(results["task3"].result_data, ["task1-result", "task2-result"])
        self.assertEqual(results["task4"].result_data, "task4-result")
        self.assertEqual(results["task5"].result_data, results["task3"].result_data)
        self.assertEqual(results["task6"].result_data, results["task3"].result_data)
        self.assertEqual(results["task7"].result_data, ["task2-result"] + results["task3"].result_data)


class TestDagTaskerWithConduit_TrivialAndNonTrivialScenarios(unittest.TestCase):
    def setUp(self):
        self.dag_tasker = DagTasker()

        pickle_serializer = PickleSerializer()

        self.res_ops = LocalResultOperations.create_with_temp_location(pickle_serializer)
        self.res_io = self.res_ops.result_io
        self.conduit = AsyncConduit(self.dag_tasker._dag, self.res_io)

        self.addCleanup(self.cleanup)

    def _conduit_start(self):
        self.conduit.start()

    def cleanup(self):
        try:
            self.res_ops.delete_location()
        except Exception as err:
            _logger.warning(err)

    def test_function_with_positional_arguments(self):
        counter_1 = 0
        counter_2 = 0

        @self.dag_tasker.task("Hello World")
        def task1(arg1):
            nonlocal counter_1
            counter_1 += 1
            return "task1-result"

        @self.dag_tasker.task()
        def task2():
            nonlocal counter_2
            counter_2 += 1

            # Not Cached
            task1("Foo Bar")
            task1("New Arg 1")
            task1("New Arg 2")
            task1("New Arg 3")

            # Cached
            task1("Hello World")
            task1("Foo Bar")
            return None

        @self.dag_tasker.task()
        def task3():
            res = task2()  # Cached
            return res

        self._conduit_start()
        self.assertEqual(counter_1, 5)
        self.assertEqual(counter_2, 1)
        self.cleanup()

    def test_consistent_result_data_when_some_functions_are_slow(self):
        counter = {
            "task1": 0,
            "task2": 0,
            "task3": 0,
        }

        @self.dag_tasker.task()
        def task1():
            time.sleep(1)
            nonlocal counter
            counter["task1"] += 1
            return [1, 2]

        @self.dag_tasker.task()
        def task2():
            nonlocal counter
            counter["task2"] += 1

            time.sleep(2)

            task1()  # Cached
            return [3, 4]

        @self.dag_tasker.task()
        def task3():
            nonlocal counter
            counter["task3"] += 1
            return task1() + task2()

        pickle_serializer = PickleSerializer()
        self.res_ops = LocalResultOperations.create_with_temp_location(pickle_serializer)
        self.res_io = self.res_ops.result_io
        self.conduit = AsyncConduit(self.dag_tasker._dag, self.res_io)
        self.dag_tasker.start(self.conduit)

        results = self.dag_tasker.results

        for _, count in counter.items():
            self.assertEqual(count, 1)

        self.assertEqual(results["task1"].result_data, [1, 2])
        self.assertEqual(results["task2"].result_data, [3, 4])
        self.assertEqual(results["task3"].result_data, [1, 2, 3, 4])


class TestDagTaskerWithConduit_ErrorHandling(unittest.TestCase):
    def setUp(self):
        self.dag_tasker = DagTasker()

        pickle_serializer = PickleSerializer()

        self.res_ops = LocalResultOperations.create_with_temp_location(pickle_serializer)
        self.res_io = self.res_ops.result_io
        self.conduit = AsyncConduit(self.dag_tasker._dag, self.res_io)

        self.addCleanup(self.cleanup)

    def cleanup(self):
        try:
            self.res_ops.delete_location()
        except Exception as err:
            _logger.warning(err)

    def test_add_task_using_same_function_names(self):
        @self.dag_tasker.task()
        def task1():
            return "task1-result"

        with self.assertRaises(ValueError):
            @self.dag_tasker.task()
            def task1():
                return "task2-result"

        self.conduit.start()

    def test_with_error_in_function(self):
        @self.dag_tasker.task()
        def task1():
            raise Exception("Simulated Error")

        @self.dag_tasker.task()
        def task2():
            res = task1()
            return res

        with self.assertRaises(NodeError):
            self.conduit.start()

    def test_retry_decorator(self):
        counter = 0

        @self.dag_tasker.task()
        @self.dag_tasker.retry(3)
        def task1():
            nonlocal counter
            counter += 1
            raise Exception("Simulated Error")

        @self.dag_tasker.task()
        def task2():
            return "task2-result"

        with self.assertRaises(NodeError):
            self.conduit.start()

        self.assertEqual(counter, 3)

    def test_error_propagation(self):
        @self.dag_tasker.task(raise_error=False)
        def task1():
            raise Exception("Simulated Error")

        @self.dag_tasker.task()
        def task2():
            task1()
            return "task2-result"

        with self.assertRaises(NodeError):
            self.conduit.start()

    def test_unhashable_arguments(self):
        @self.dag_tasker.task([1, 2])  # Mutable List
        def task1(arg1):
            return "task1-result"

        @self.dag_tasker.task({"key": "value"})  # Mutable Dictionary
        def task2(arg2):
            return "task2-result"

        with self.assertRaises(NodeError):
            self.conduit.start()

    def test_non_pickle_serializable_result_data(self):
        # Examples of non pickle serializable
        # Generators, Lambdas, objects with weak references, etc.

        @self.dag_tasker.task((1, 2, 3))
        def task1(tup):
            return filter(lambda x: x % 2 != 0, tup)

        f = lambda x: x**2

        @self.dag_tasker.task()
        def task2():
            return (f(x) for x in range(100))

        with self.assertRaises(ResultDataError):
            self.conduit.start()

    def test_non_json_serializable_result_data(self):
        # Examples: 
        # - Custom Function/Class Objects,
        # - File Objects,
        # - Set and Frozenset,
        # - Byte Objects

        self.dag_tasker = DagTasker()
        json_serializer = JsonSerializer()
        self.res_ops = LocalResultOperations.create_with_temp_location(json_serializer)
        self.res_io = self.res_ops.result_io
        self.conduit = AsyncConduit(self.dag_tasker._dag, self.res_io)

        # Custom class in Python are not JSON Serializable
        class CustomClass:
            pass

        # Functions in Python are not JSON Serializable
        def custom_function():
            pass

        @self.dag_tasker.task()
        def task1():
            return CustomClass()

        @self.dag_tasker.task()
        def task2():
            return custom_function()

        with self.assertRaises(ResultDataError):
            self.conduit.start()

    def test_function_with_keyword_arguments(self):
        with self.assertRaises(ValueError):
            @self.dag_tasker.task()
            def task1(kw1=1):
                return "task1-result"


if __name__ == "__main__":
    unittest.main()
