import unittest
from unittest.mock import Mock
from multi_x_serverless.deployment.client.workflow import MultiXServerlessFunction


def invoke_serverless_function(function_name, payload):
    pass


def get_predecessor_data():
    pass


class TestMultiXServerlessFunction(unittest.TestCase):
    def test_init(self):
        def function(x):
            return x

        name = "test_function"
        entry_point = True
        timeout = 10
        memory = 128
        regions_and_providers = {}

        function_obj = MultiXServerlessFunction(function, name, entry_point, timeout, memory, regions_and_providers)

        self.assertEqual(function_obj.function, function)
        self.assertEqual(function_obj.name, name)
        self.assertEqual(function_obj.entry_point, entry_point)
        self.assertEqual(function_obj.handler, function.__name__)
        self.assertEqual(function_obj.timeout, timeout)
        self.assertEqual(function_obj.memory, memory)
        self.assertIsNone(function_obj.regions_and_providers)

    def test_get_successors(self):
        def test_function(x):
            return x

        name = "test_function"
        entry_point = True
        timeout = 10
        memory = 128
        regions_and_providers = {}

        function_obj_1 = MultiXServerlessFunction(
            test_function, name, entry_point, timeout, memory, regions_and_providers
        )

        workflow = Mock()
        workflow.functions = [function_obj_1]

        self.assertEqual(function_obj_1.get_successors(workflow), [])

        def function(x):
            return invoke_serverless_function("test_function", x)

        function_obj_2 = MultiXServerlessFunction(function, name, entry_point, timeout, memory, regions_and_providers)

        workflow = Mock()
        workflow.functions = [function_obj_1, function_obj_2]

        self.assertEqual(function_obj_2.get_successors(workflow), [function_obj_1])

    def test_is_waiting_for_predecessors(self):
        def function(x):
            return x

        name = "test_function"
        entry_point = True
        timeout = 10
        memory = 128
        regions_and_providers = {}

        function_obj = MultiXServerlessFunction(function, name, entry_point, timeout, memory, regions_and_providers)

        self.assertFalse(function_obj.is_waiting_for_predecessors())

        def function(x):
            return get_predecessor_data()

        function_obj = MultiXServerlessFunction(function, name, entry_point, timeout, memory, regions_and_providers)

        self.assertTrue(function_obj.is_waiting_for_predecessors())


if __name__ == "__main__":
    unittest.main()
