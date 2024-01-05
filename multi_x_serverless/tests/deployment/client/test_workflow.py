import unittest
from unittest.mock import Mock
from typing import Any
from multi_x_serverless.deployment.client.workflow import MultiXServerlessFunction, MultiXServerlessWorkflow
from types import FrameType


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
        regions_and_providers = {"only_regions": ["aws:us-east-1"], "forbidden_regions": ["aws:us-east-2"]}
        providers = [{"name": "aws", "config": {"timeout": 60, "memory": 128}}]

        function_obj = MultiXServerlessFunction(function, name, entry_point, regions_and_providers, providers)

        self.assertEqual(function_obj.function, function)
        self.assertEqual(function_obj.name, name)
        self.assertEqual(function_obj.entry_point, entry_point)
        self.assertEqual(function_obj.handler, function.__name__)
        self.assertEqual(function_obj.regions_and_providers, regions_and_providers)
        self.assertEqual(function_obj.providers, providers)

    def test_get_successors(self):
        def test_function(x):
            return x

        name = "test_function"
        entry_point = True
        regions_and_providers = {}
        providers = []

        function_obj_1 = MultiXServerlessFunction(test_function, name, entry_point, regions_and_providers, providers)

        workflow = Mock()
        workflow.functions = [function_obj_1]

        self.assertEqual(function_obj_1.get_successors(workflow), [])

        def function(x):
            return invoke_serverless_function("test_function", x)

        function_obj_2 = MultiXServerlessFunction(function, name, entry_point, regions_and_providers, providers)

        workflow = Mock()
        workflow.functions = {"test_function": function_obj_1, "test_function_2": function_obj_2}

        self.assertEqual(function_obj_2.get_successors(workflow), [function_obj_1])

    def test_is_waiting_for_predecessors(self):
        def function(x):
            return x

        name = "test_function"
        entry_point = True
        regions_and_providers = {}
        providers = []

        function_obj = MultiXServerlessFunction(function, name, entry_point, regions_and_providers, providers)

        self.assertFalse(function_obj.is_waiting_for_predecessors())

        def function(x):
            return get_predecessor_data()

        function_obj = MultiXServerlessFunction(function, name, entry_point, regions_and_providers, providers)

        self.assertTrue(function_obj.is_waiting_for_predecessors())

    def test_serverless_function(self):
        workflow = MultiXServerlessWorkflow(
            name="test-workflow"
        )  # Assuming Workflow is the class containing serverless_function
        workflow.register_function = Mock()
        workflow.get_routing_decision_from_platform = Mock(return_value={"decision": 1})

        @workflow.serverless_function(name="test_func", entry_point=True)
        def test_func(payload):
            return payload * 2

        # Check if the function was registered correctly
        args, _ = workflow.register_function.call_args
        registered_func = args[0]
        self.assertEqual(registered_func.__name__, "test_func")
        self.assertEqual(args[1:], ("test_func", True, {}, []))

        self.assertEqual(test_func.routing_decision, {})

        self.assertEqual(test_func(2), 4)

        # Check if the routing_decision attribute was set correctly
        self.assertEqual(test_func.routing_decision, {"decision": 1})

    def test_invoke_serverless_function(self):
        workflow = MultiXServerlessWorkflow(name="test-workflow")
        workflow.register_function = Mock()

        @workflow.serverless_function(name="test_func")
        def test_func(payload: dict[str, Any]) -> dict[str, Any]:
            return payload * 2

        # Check if the function was registered correctly
        args, _ = workflow.register_function.call_args
        registered_func = args[0]
        self.assertEqual(registered_func.__name__, "test_func")
        self.assertEqual(args[1:], ("test_func", False, {}, []))

        self.assertEqual(test_func.routing_decision, {})

        self.assertEqual(
            test_func(
                '{"payload": 2, "routing_decision": {"routing_placement": {"test_instance_1": {"provider_region": "aws:region", "identifier": "test_identifier"}}, "current_instance_name": "test_instance", "instances": [{"instance_name": "test_instance", "succeeding_instances": ["test_instance_1"]}]}}'
            ),
            4,
        )

        # Check if the routing_decision attribute was set correctly
        self.assertEqual(
            test_func.routing_decision,
            {
                "routing_placement": {
                    "test_instance_1": {"provider_region": "aws:region", "identifier": "test_identifier"}
                },
                "current_instance_name": "test_instance_1",
                "instances": [{"instance_name": "test_instance", "succeeding_instances": ["test_instance_1"]}],
            },
        )

    def test_invoke_serverless_function(self):
        workflow = MultiXServerlessWorkflow(name="test-workflow")
        workflow.register_function = Mock()
        workflow.invoke_function_through_sns = Mock()

        @workflow.serverless_function(name="test_func")
        def test_func(payload: dict[str, Any]) -> dict[str, Any]:
            # Call invoke_serverless_function from within test_func
            workflow.invoke_serverless_function(test_func, payload)

            return "Some response"

        # Check if the function was registered correctly
        args, _ = workflow.register_function.call_args
        registered_func = args[0]
        registered_func.name = "test_func"
        self.assertEqual(registered_func.__name__, "test_func")
        self.assertEqual(args[1:], ("test_func", False, {}, []))
        workflow.functions["test_func"] = registered_func

        # Call test_func with a payload
        response = test_func(
            '{"payload": 2, "routing_decision": {"routing_placement": {"test_func": {"provider_region": "aws:region", "identifier": "test_identifier"}, "test_func_1": {"provider_region": "aws:region", "identifier": "test_identifier"}}, "current_instance_name": "test_func", "instances": [{"instance_name": "test_func", "succeeding_instances": ["test_func_1"]}]}}'
        )

        # Check if invoke_serverless_function was called with the correct arguments
        workflow.invoke_function_through_sns.assert_called_once_with(
            '{"payload": 2, "routing_decision": {"routing_placement": {"test_func": {"provider_region": "aws:region", "identifier": "test_identifier"}, "test_func_1": {"provider_region": "aws:region", "identifier": "test_identifier"}}, "current_instance_name": "test_func_1", "instances": [{"instance_name": "test_func", "succeeding_instances": ["test_func_1"]}]}}',
            "region",
            "test_identifier",
        )

        # Check if the response from invoke_serverless_function is correct
        self.assertEqual(response, "Some response")

    def test_invoke_serverless_function_json_argument(self):
        workflow = MultiXServerlessWorkflow(name="test-workflow")
        workflow.register_function = Mock()
        workflow.invoke_function_through_sns = Mock()

        @workflow.serverless_function(name="test_func")
        def test_func(payload: str) -> dict[str, Any]:
            # Call invoke_serverless_function from within test_func
            workflow.invoke_serverless_function(test_func, payload)

            return "Some response"

        # Check if the function was registered correctly
        args, _ = workflow.register_function.call_args
        registered_func = args[0]
        registered_func.name = "test_func"
        self.assertEqual(registered_func.__name__, "test_func")
        self.assertEqual(args[1:], ("test_func", False, {}, []))
        workflow.functions["test_func"] = registered_func

        # Call test_func with a payload
        response = test_func(
            r'{"payload": "{\"key\": \"value\"}", "routing_decision": {"routing_placement": {"test_func": {"provider_region": "aws:region", "identifier": "test_identifier"}, "test_func_1": {"provider_region": "aws:region", "identifier": "test_identifier"}}, "current_instance_name": "test_func", "instances": [{"instance_name": "test_func", "succeeding_instances": ["test_func_1"]}]}}'
        )

        # Check if invoke_serverless_function was called with the correct arguments
        workflow.invoke_function_through_sns.assert_called_once_with(
            r'{"payload": "{\"key\": \"value\"}", "routing_decision": {"routing_placement": {"test_func": {"provider_region": "aws:region", "identifier": "test_identifier"}, "test_func_1": {"provider_region": "aws:region", "identifier": "test_identifier"}}, "current_instance_name": "test_func_1", "instances": [{"instance_name": "test_func", "succeeding_instances": ["test_func_1"]}]}}',
            "region",
            "test_identifier",
        )

        # Check if the response from invoke_serverless_function is correct
        self.assertEqual(response, "Some response")

    def test_get_routing_decision(self):
        workflow = MultiXServerlessWorkflow(name="test-workflow")
        # Test when 'wrapper' is in frame.f_locals and wrapper has 'routing_decision' attribute
        frame = Mock(spec=FrameType)
        frame.f_locals = {"wrapper": Mock(routing_decision="decision")}
        self.assertEqual(workflow.get_routing_decision(frame), "decision")

        # Test when 'wrapper' is not in frame.f_locals
        frame.f_locals = {}
        with self.assertRaises(RuntimeError):
            workflow.get_routing_decision(frame)

        # Test when 'wrapper' is in frame.f_locals but wrapper does not have 'routing_decision' attribute
        mock_wrapper = Mock()
        mock_wrapper.routing_decision = None
        frame.f_locals = {"wrapper": mock_wrapper}
        self.assertEqual(workflow.get_routing_decision(frame), None)

    def test_get_function__name__from_frame(self):
        workflow = MultiXServerlessWorkflow(name="test-workflow")
        # Test when '__name__' is in frame.f_locals
        frame = Mock(spec=FrameType)
        frame.f_code.co_name = "function_name"
        self.assertEqual(workflow.get_function__name__from_frame(frame), "function_name")

    def test_is_entry_point(self):
        workflow = MultiXServerlessWorkflow(name="test-workflow")
        # Test when 'wrapper' is in frame.f_locals and wrapper has 'entry_point' attribute
        frame = Mock(spec=FrameType)
        frame.f_locals = {"wrapper": Mock(entry_point=True)}
        self.assertTrue(workflow.is_entry_point(frame))

        # Test when 'wrapper' is not in frame.f_locals
        frame.f_locals = {}
        with self.assertRaises(RuntimeError):
            workflow.is_entry_point(frame)

        # Test when 'wrapper' is in frame.f_locals but wrapper does not have 'entry_point' attribute
        wrapper_mock = Mock()
        wrapper_mock.entry_point = False
        frame.f_locals = {"wrapper": wrapper_mock}
        self.assertFalse(workflow.is_entry_point(frame))

    def test_get_next_instance_name(self):
        workflow = MultiXServerlessWorkflow(name="test-workflow")
        routing_decision = {
            "instances": [
                {
                    "instance_name": "current_instance",
                    "succeeding_instances": ["successor_function:merge", "successor_function:0_1"],
                },
                {"instance_name": "other_instance", "succeeding_instances": ["other_successor_function:merge"]},
            ]
        }
        current_instance_name = "current_instance"
        successor_function_name = "successor_function"

        next_instance_name = workflow.get_next_instance_name(
            current_instance_name, routing_decision, successor_function_name
        )

        self.assertEqual(next_instance_name, "successor_function:merge")

    def test_get_next_instance_name_non_merge_successor(self):
        workflow = MultiXServerlessWorkflow(name="test-workflow")
        routing_decision = {
            "instances": [
                {
                    "instance_name": "current_instance",
                    "succeeding_instances": [
                        "successor_function:current_instance_0_0",
                        "successor_function:current_instance_0_1",
                    ],
                },
                {"instance_name": "other_instance", "succeeding_instances": ["other_successor_function:merge"]},
            ]
        }
        current_instance_name = "current_instance"
        successor_function_name = "successor_function"

        # Test with _successor_index = 0
        workflow._successor_index = 0
        next_instance_name = workflow.get_next_instance_name(
            current_instance_name, routing_decision, successor_function_name
        )
        self.assertEqual(next_instance_name, "successor_function:current_instance_0_0")

        # The _successor_index should be incremented
        next_instance_name = workflow.get_next_instance_name(
            current_instance_name, routing_decision, successor_function_name
        )
        self.assertEqual(next_instance_name, "successor_function:current_instance_0_1")


if __name__ == "__main__":
    unittest.main()
