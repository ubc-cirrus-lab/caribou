import unittest
from unittest.mock import Mock
from typing import Any
from multi_x_serverless.deployment.client.multi_x_serverless_workflow import (
    MultiXServerlessWorkflow,
)
from multi_x_serverless.deployment.client.multi_x_serverless_function import (
    MultiXServerlessFunction,
)
from types import FrameType


def invoke_serverless_function(function_name, payload):
    pass


class TestMultiXServerlessWorkflow(unittest.TestCase):
    def test_serverless_function(self):
        workflow = MultiXServerlessWorkflow(
            name="test-workflow"
        )  # Assuming Workflow is the class containing serverless_function
        workflow.register_function = Mock()
        workflow.get_routing_decision_from_platform = Mock(return_value={"decision": 1})

        @workflow.serverless_function(
            name="test_func",
            entry_point=True,
            regions_and_providers={
                "only_regions": [["aws", "us-east-1"]],
                "forbidden_regions": [["aws", "us-east-2"]],
                "providers": [
                    {
                        "name": "aws",
                        "config": {
                            "timeout": 60,
                            "memory": 128,
                        },
                    }
                ],
            },
        )
        def test_func(payload):
            return payload * 2

        # Check if the function was registered correctly
        args, _ = workflow.register_function.call_args
        registered_func = args[0]
        self.assertEqual(registered_func.__name__, "test_func")
        self.assertEqual(
            args[1:],
            (
                "test_func",
                True,
                {
                    "only_regions": [["aws", "us-east-1"]],
                    "forbidden_regions": [["aws", "us-east-2"]],
                    "providers": [
                        {
                            "name": "aws",
                            "config": {
                                "timeout": 60,
                                "memory": 128,
                            },
                        }
                    ],
                },
            ),
        )

        self.assertEqual(test_func.routing_decision, {})

        self.assertEqual(test_func(2), 4)

        # Check if the routing_decision attribute was set correctly
        self.assertEqual(test_func.routing_decision["decision"], 1)

    def test_invoke_serverless_function_simple(self):
        workflow = MultiXServerlessWorkflow(name="test-workflow")
        workflow.register_function = Mock()

        @workflow.serverless_function(name="test_func")
        def test_func(payload: dict[str, Any]) -> dict[str, Any]:
            return payload * 2

        # Check if the function was registered correctly
        args, _ = workflow.register_function.call_args
        registered_func = args[0]
        self.assertEqual(registered_func.__name__, "test_func")
        self.assertEqual(args[1:], ("test_func", False, {}))

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
                "current_instance_name": "test_instance",
                "instances": [{"instance_name": "test_instance", "succeeding_instances": ["test_instance_1"]}],
            },
        )

    def test_invoke_serverless_function_invoke_second_invocation(self):
        workflow = MultiXServerlessWorkflow(name="test-workflow")
        workflow.register_function = Mock()
        mock_remote_client = Mock()
        mock_remote_client.invoke_function = Mock(return_value={"statusCode": 200, "body": "Some response"})
        mock_remote_client_factory = Mock()
        mock_remote_client_factory.get_remote_client = Mock(return_value=mock_remote_client)
        workflow._remote_client_factory = mock_remote_client_factory

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
        self.assertEqual(args[1:], ("test_func", False, {}))
        workflow.functions["test_func"] = registered_func

        # Call test_func with a payload
        response = test_func(
            '{"payload": 2, "routing_decision": {"run_id": "123", "routing_placement": {"test_func": {"provider_region": "aws:region", "identifier": "test_identifier"}, "test_func_1::": {"provider_region": "aws:region", "identifier": "test_identifier"}}, "current_instance_name": "test_func", "instances": [{"instance_name": "test_func", "succeeding_instances": ["test_func_1::"]}]}}'
        )

        mock_remote_client_factory.get_remote_client.assert_called_once_with("aws", "region")

        # Check if invoke_serverless_function was called with the correct arguments
        mock_remote_client.invoke_function.assert_called_once_with(
            message='{"payload": 2, "routing_decision": {"run_id": "123", "routing_placement": {"test_func": {"provider_region": "aws:region", "identifier": "test_identifier"}, "test_func_1::": {"provider_region": "aws:region", "identifier": "test_identifier"}}, "current_instance_name": "test_func_1::", "instances": [{"instance_name": "test_func", "succeeding_instances": ["test_func_1::"]}]}}',
            identifier="test_identifier",
            workflow_instance_id="123",
            merge=False,
            function_name=None,
            expected_counter=-1,
        )

        # Check if the response from invoke_serverless_function is correct
        self.assertEqual(response, "Some response")

    def test_invoke_serverless_function_with_merge_successor(self):
        workflow = MultiXServerlessWorkflow(name="test-workflow")
        workflow.register_function = Mock()
        mock_remote_client = Mock()
        mock_remote_client.invoke_function = Mock(return_value={"statusCode": 200, "body": "Some response"})
        mock_remote_client_factory = Mock()
        mock_remote_client_factory.get_remote_client = Mock(return_value=mock_remote_client)
        workflow._remote_client_factory = mock_remote_client_factory

        @workflow.serverless_function(name="test_func")
        def test_func(payload: dict[str, Any]) -> dict[str, Any]:
            # Call invoke_serverless_function from within test_func
            workflow.invoke_serverless_function(merge_func, payload)

            return "Some response"

        # Check if the function was registered correctly
        args, _ = workflow.register_function.call_args
        registered_func = args[0]
        registered_func.name = "test_func"
        self.assertEqual(registered_func.__name__, "test_func")
        self.assertEqual(args[1:], ("test_func", False, {}))
        workflow.functions["test_func"] = registered_func

        @workflow.serverless_function(name="merge_func")
        def merge_func(payload: dict[str, Any]) -> dict[str, Any]:
            return "Some response"

        # Check if the function was registered correctly
        args, _ = workflow.register_function.call_args
        registered_func = args[0]
        registered_func.name = "merge_func"
        self.assertEqual(registered_func.__name__, "merge_func")
        self.assertEqual(args[1:], ("merge_func", False, {}))
        workflow.functions["merge_func"] = registered_func

        # Call test_func with a payload
        response = test_func(
            '{"payload": 2, "routing_decision": {"run_id": "123", "routing_placement": {"test_func": {"provider_region": "aws:region", "identifier": "test_identifier"}, "merge_func:merge:": {"provider_region": "aws:region", "identifier": "test_identifier"}}, "current_instance_name": "test_func", "instances": [{"instance_name": "test_func", "succeeding_instances": ["merge_func:merge:"]}, {"instance_name": "merge_func:merge:", "preceding_instances": ["test_func"]}]}}'
        )

        mock_remote_client_factory.get_remote_client.assert_called_once_with("aws", "region")

        # Check if invoke_serverless_function was called with the correct arguments
        mock_remote_client.invoke_function.assert_called_once_with(
            message='{"payload": 2, "routing_decision": {"run_id": "123", "routing_placement": {"test_func": {"provider_region": "aws:region", "identifier": "test_identifier"}, "merge_func:merge:": {"provider_region": "aws:region", "identifier": "test_identifier"}}, "current_instance_name": "merge_func:merge:", "instances": [{"instance_name": "test_func", "succeeding_instances": ["merge_func:merge:"]}, {"instance_name": "merge_func:merge:", "preceding_instances": ["test_func"]}]}}',
            identifier="test_identifier",
            workflow_instance_id="123",
            merge=True,
            function_name="merge_func",
            expected_counter=1,
        )

        # Check if the response from invoke_serverless_function is correct
        self.assertEqual(response, "Some response")

    def test_invoke_serverless_function_with_multiple_merge_successor(self):
        workflow = MultiXServerlessWorkflow(name="test-workflow")
        workflow.register_function = Mock()
        mock_remote_client = Mock()
        mock_remote_client.invoke_function = Mock(return_value={"statusCode": 200, "body": "Some response"})
        mock_remote_client_factory = Mock()
        mock_remote_client_factory.get_remote_client = Mock(return_value=mock_remote_client)
        workflow._remote_client_factory = mock_remote_client_factory

        @workflow.serverless_function(name="test_func")
        def test_func(payload: dict[str, Any]) -> dict[str, Any]:
            # Call invoke_serverless_function from within test_func
            workflow.invoke_serverless_function(merge_func, payload)

            return "Some response"

        # Check if the function was registered correctly
        args, _ = workflow.register_function.call_args
        registered_func = args[0]
        registered_func.name = "test_func"
        self.assertEqual(registered_func.__name__, "test_func")
        self.assertEqual(args[1:], ("test_func", False, {}))
        workflow.functions["test_func"] = registered_func

        @workflow.serverless_function(name="test_func2")
        def test_func2(payload: dict[str, Any]) -> dict[str, Any]:
            # Call invoke_serverless_function from within test_func
            workflow.invoke_serverless_function(merge_func, payload)

            return "Some response"

        # Check if the function was registered correctly
        args, _ = workflow.register_function.call_args
        registered_func = args[0]
        registered_func.name = "test_func2"
        self.assertEqual(registered_func.__name__, "test_func2")
        self.assertEqual(args[1:], ("test_func2", False, {}))
        workflow.functions["test_func2"] = registered_func

        @workflow.serverless_function(name="merge_func")
        def merge_func(payload: dict[str, Any]) -> dict[str, Any]:
            return "Some response"

        # Check if the function was registered correctly
        args, _ = workflow.register_function.call_args
        registered_func = args[0]
        registered_func.name = "merge_func"
        self.assertEqual(registered_func.__name__, "merge_func")
        self.assertEqual(args[1:], ("merge_func", False, {}))
        workflow.functions["merge_func"] = registered_func

        # Call test_func with a payload
        response = test_func(
            '{"payload": 2, "routing_decision": {"run_id": "123", "routing_placement": {"test_func": {"provider_region": "aws:region", "identifier": "test_identifier"}, "test_func2": {"provider_region": "aws:region", "identifier": "test_identifier"}, "merge_func:merge:": {"provider_region": "aws:region", "identifier": "test_identifier"}}, "current_instance_name": "test_func", "instances": [{"instance_name": "test_func", "succeeding_instances": ["merge_func:merge:"]}, {"instance_name": "test_func2", "succeeding_instances": ["merge_func:merge:"]}, {"instance_name": "merge_func:merge:", "preceding_instances": ["test_func", "test_func2"]}]}}'
        )

        mock_remote_client_factory.get_remote_client.assert_called_once_with("aws", "region")

        # Check if invoke_serverless_function was called with the correct arguments
        mock_remote_client.invoke_function.assert_called_once_with(
            message='{"payload": 2, "routing_decision": {"run_id": "123", "routing_placement": {"test_func": {"provider_region": "aws:region", "identifier": "test_identifier"}, "test_func2": {"provider_region": "aws:region", "identifier": "test_identifier"}, "merge_func:merge:": {"provider_region": "aws:region", "identifier": "test_identifier"}}, "current_instance_name": "merge_func:merge:", "instances": [{"instance_name": "test_func", "succeeding_instances": ["merge_func:merge:"]}, {"instance_name": "test_func2", "succeeding_instances": ["merge_func:merge:"]}, {"instance_name": "merge_func:merge:", "preceding_instances": ["test_func", "test_func2"]}]}}',
            identifier="test_identifier",
            workflow_instance_id="123",
            merge=True,
            function_name="merge_func",
            expected_counter=2,
        )

        # Check if the response from invoke_serverless_function is correct
        self.assertEqual(response, "Some response")

    def test_invoke_serverless_function_json_argument(self):
        workflow = MultiXServerlessWorkflow(name="test-workflow")
        workflow.register_function = Mock()
        mock_remote_client = Mock()
        mock_remote_client.invoke_function = Mock(return_value={"statusCode": 200, "body": "Some response"})
        mock_remote_client_factory = Mock()
        mock_remote_client_factory.get_remote_client = Mock(return_value=mock_remote_client)
        workflow._remote_client_factory = mock_remote_client_factory

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
        self.assertEqual(args[1:], ("test_func", False, {}))
        workflow.functions["test_func"] = registered_func

        # Call test_func with a payload
        response = test_func(
            r'{"payload": "{\"key\": \"value\"}", "routing_decision": {"run_id": "123", "routing_placement": {"test_func": {"provider_region": "aws:region", "identifier": "test_identifier"}, "test_func_1::": {"provider_region": "aws:region", "identifier": "test_identifier"}}, "current_instance_name": "test_func", "instances": [{"instance_name": "test_func", "succeeding_instances": ["test_func_1::"]}]}}'
        )

        mock_remote_client_factory.get_remote_client.assert_called_once_with("aws", "region")

        # Check if invoke_serverless_function was called with the correct arguments
        mock_remote_client.invoke_function.assert_called_once_with(
            message=r'{"payload": "{\"key\": \"value\"}", "routing_decision": {"run_id": "123", "routing_placement": {"test_func": {"provider_region": "aws:region", "identifier": "test_identifier"}, "test_func_1::": {"provider_region": "aws:region", "identifier": "test_identifier"}}, "current_instance_name": "test_func_1::", "instances": [{"instance_name": "test_func", "succeeding_instances": ["test_func_1::"]}]}}',
            identifier="test_identifier",
            workflow_instance_id="123",
            merge=False,
            function_name=None,
            expected_counter=-1,
        )

        # Check if the response from invoke_serverless_function is correct
        self.assertEqual(response, "Some response")

    def test_get_successors(self):
        def test_function(x):
            return x

        name = "test_function"
        entry_point = True
        regions_and_providers = {}
        providers = []

        function_obj_1 = MultiXServerlessFunction(test_function, name, entry_point, regions_and_providers)

        workflow = MultiXServerlessWorkflow(name="test-workflow")
        workflow.functions = [function_obj_1]

        self.assertEqual(workflow.get_successors(function_obj_1), [])

        def function(x):
            return invoke_serverless_function("test_function", x)

        function_obj_2 = MultiXServerlessFunction(function, name, entry_point, regions_and_providers)

        workflow.functions = {"test_function": function_obj_1, "test_function_2": function_obj_2}

        self.assertEqual(workflow.get_successors(function_obj_2), [function_obj_1])

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
