import unittest
from unittest.mock import Mock
from multi_x_serverless.deployment.client.multi_x_serverless_workflow import MultiXServerlessFunction


def get_predecessor_data():
    pass


class TestMultiXServerlessFunction(unittest.TestCase):
    def test_init(self):
        def function(x):
            return x

        name = "test_function"
        entry_point = True
        providers = [{"name": "aws", "config": {"timeout": 60, "memory": 128}}]
        regions_and_providers = {
            "only_regions": [
                {
                    "provider": "aws",
                    "region": "us-east-1",
                }
            ],
            "forbidden_regions": [
                {
                    "provider": "aws",
                    "region": "us-east-2",
                }
            ],
            "providers": providers,
        }
        environment_variables = [{"key": "example_key", "value": "example_value"}]

        function_obj = MultiXServerlessFunction(
            function, name, entry_point, regions_and_providers, environment_variables
        )

        self.assertEqual(function_obj.function_callable, function)
        self.assertEqual(function_obj.name, name)
        self.assertEqual(function_obj.entry_point, entry_point)
        self.assertEqual(function_obj.handler, function.__name__)
        self.assertEqual(function_obj.regions_and_providers, regions_and_providers)
        self.assertEqual(function_obj.environment_variables, environment_variables)

    def test_is_waiting_for_predecessors(self):
        def function(x):
            return x

        name = "test_function"
        entry_point = True
        regions_and_providers = {}
        environment_variables = []

        function_obj = MultiXServerlessFunction(
            function, name, entry_point, regions_and_providers, environment_variables
        )

        self.assertFalse(function_obj.is_waiting_for_predecessors())

        def function(x):
            return get_predecessor_data()

        function_obj = MultiXServerlessFunction(
            function, name, entry_point, regions_and_providers, environment_variables
        )

        self.assertTrue(function_obj.is_waiting_for_predecessors())

    def test_validate_function_name(self):
        def function(x):
            return x

        name = "test_function"
        entry_point = True
        regions_and_providers = {}
        environment_variables = []

        function_obj = MultiXServerlessFunction(
            function, name, entry_point, regions_and_providers, environment_variables
        )

        function_obj.validate_function_name()

        function_obj.name = "test:function"

        with self.assertRaises(ValueError):
            function_obj.validate_function_name()


if __name__ == "__main__":
    unittest.main()