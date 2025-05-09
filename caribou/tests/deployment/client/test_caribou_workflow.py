from concurrent.futures import Future
import os
import unittest
from datetime import datetime
from unittest.mock import Mock, patch
from typing import Any
from caribou.deployment.client.caribou_workflow import (
    CaribouWorkflow,
    CustomDecoder,
    CustomEncoder,
)
from caribou.deployment.client.caribou_function import (
    CaribouFunction,
)
import inspect
from types import FrameType
from caribou.common.constants import (
    GLOBAL_TIME_ZONE,
    HOME_REGION_THRESHOLD,
    LOG_VERSION,
    MAX_TRANSFER_SIZE,
    MAX_WORKERS,
    MAXIMUM_HOPS_FROM_CLIENT_REQUEST,
    TIME_FORMAT,
    WORKFLOW_PLACEMENT_DECISION_TABLE,
)


def invoke_serverless_function(function_name, payload):
    pass


class MockFrame:
    def __init__(self, f_back):
        self.f_back = f_back
        self.f_locals = {}


class TestCaribouWorkflow(unittest.TestCase):
    def setUp(self):
        self.workflow = CaribouWorkflow(name="test-workflow", version="0.0.1")
        self.workflow.get_cpu_info = Mock(return_value="test_cpu_info")
        self.workflow.get_current_instance_provider_region_instance_name = Mock(
            return_value=("provider1", "region", "test_func", "123")
        )

    def test_serverless_function(self):
        self.workflow.register_function = Mock()
        self.workflow.get_workflow_placement_decision_from_platform = Mock(
            return_value=({"current_instance_name": "test_func", "instances": [], "workflow_placement": {}}, 0.0, 0.0)
        )
        self.workflow._need_to_redirect = Mock(return_value=False)

        @self.workflow.serverless_function(
            name="test_func",
            entry_point=True,
            regions_and_providers={
                "allowed_regions": [
                    {
                        "provider": "provider1",
                        "region": "region2",
                    }
                ],
                "disallowed_regions": [
                    {
                        "provider": "provider1",
                        "region": "region3",
                    }
                ],
                "providers": {
                    "provider1": {
                        "config": {
                            "timeout": 60,
                            "memory": 128,
                        },
                    },
                },
            },
        )
        def test_func(payload):
            return payload * 2

        # Check if the function was registered correctly
        args, _ = self.workflow.register_function.call_args
        registered_func = args[0]
        self.assertEqual(registered_func.__name__, "test_func")
        self.assertEqual(
            args[1:],
            (
                "test_func",
                True,
                {
                    "allowed_regions": [
                        {
                            "provider": "provider1",
                            "region": "region2",
                        }
                    ],
                    "disallowed_regions": [
                        {
                            "provider": "provider1",
                            "region": "region3",
                        }
                    ],
                    "providers": {
                        "provider1": {
                            "config": {
                                "timeout": 60,
                                "memory": 128,
                            },
                        },
                    },
                },
                [],
                False,
            ),
        )

        self.assertEqual(self.workflow._current_workflow_placement_decision, {})

        argument_raw = {"Records": [{"Sns": {"Message": '{"payload": 2}'}}]}

        self.assertEqual(test_func(argument_raw), 4)

        self.assertEqual(
            self.workflow._current_workflow_placement_decision["current_instance_name"],
            "test_func",
        )

    def test_serverless_function_with_environment_variables(self):
        self.workflow.register_function = Mock()
        self.workflow.get_workflow_placement_decision_from_platform = Mock(return_value={"decision": 1})

        @self.workflow.serverless_function(
            name="test_func",
            entry_point=True,
            regions_and_providers={
                "allowed_regions": [["provider1", "region2"]],
                "disallowed_regions": [["provider1", "region3"]],
                "providers": {
                    "provider1": {
                        "config": {
                            "timeout": 60,
                            "memory": 128,
                        },
                    },
                },
            },
            environment_variables=[
                {"key": "example_key", "value": "example_value"},
                {"key": "example_key_2", "value": "example_value_2"},
                {"key": "example_key_3", "value": "example_value_3"},
            ],
        )
        def test_func(payload):
            return payload * 2

        args, _ = self.workflow.register_function.call_args

        # Test with multiple environment variables
        self.assertEqual(
            args[1:],
            (
                "test_func",
                True,
                {
                    "allowed_regions": [["provider1", "region2"]],
                    "disallowed_regions": [["provider1", "region3"]],
                    "providers": {
                        "provider1": {
                            "config": {
                                "timeout": 60,
                                "memory": 128,
                            },
                        },
                    },
                },
                [
                    {"key": "example_key", "value": "example_value"},
                    {"key": "example_key_2", "value": "example_value_2"},
                    {"key": "example_key_3", "value": "example_value_3"},
                ],
                False,
            ),
        )

    def test_invoke_serverless_function_simple(self):
        self.workflow.register_function = Mock()

        @self.workflow.serverless_function(name="test_func")
        def test_func(payload: dict[str, Any]) -> dict[str, Any]:
            return payload * 2

        # Check if the function was registered correctly
        args, _ = self.workflow.register_function.call_args
        registered_func = args[0]
        self.assertEqual(registered_func.__name__, "test_func")
        self.assertEqual(args[1:], ("test_func", False, {}, [], False))

        self.assertEqual(self.workflow._current_workflow_placement_decision, {})

        self.assertEqual(
            test_func(
                {
                    "payload": 2,
                    "workflow_placement_decision": {
                        "run_id": "123",
                        "workflow_placement": {
                            "test_instance_1": {
                                "provider_region": {"provider": "provider1", "region": "region"},
                                "identifier": "test_identifier",
                            }
                        },
                        "current_instance_name": "test_instance",
                        "instances": {
                            "test_instance": {
                                "instance_name": "test_instance",
                                "succeeding_instances": ["test_instance_1"],
                            }
                        },
                    },
                }
            ),
            4,
        )

        # Check if the workflow_placement_decision attribute was set correctly
        self.assertEqual(
            self.workflow._current_workflow_placement_decision,
            {
                "workflow_placement": {
                    "test_instance_1": {
                        "provider_region": {"provider": "provider1", "region": "region"},
                        "identifier": "test_identifier",
                    }
                },
                "current_instance_name": "test_instance",
                "instances": {
                    "test_instance": {"instance_name": "test_instance", "succeeding_instances": ["test_instance_1"]}
                },
                "run_id": "123",
            },
        )

    def test_invoke_serverless_function_invoke_second_invocation(self):
        self.workflow.register_function = Mock()
        mock_remote_client = Mock()

        mock_remote_client.invoke_function = Mock(return_value=(0.0, 0.0, True, 0.0, 0.0))

        mock_uuid = Mock()
        mock_uuid.hex = "37a5262"
        with patch("caribou.deployment.client.caribou_workflow.RemoteClientFactory") as mock_factory_class:
            mock_factory_class.get_remote_client.return_value = mock_remote_client

            with patch("uuid.uuid4", return_value=mock_uuid):

                @self.workflow.serverless_function(name="test_func")
                def test_func(payload: dict[str, Any]) -> dict[str, Any]:
                    # Call invoke_serverless_function from within test_func
                    self.workflow.invoke_serverless_function(test_func, payload)

                    return "Some response"

                # Check if the function was registered correctly
                args, _ = self.workflow.register_function.call_args
                registered_func = args[0]
                registered_func.name = "test_func"
                self.assertEqual(registered_func.__name__, "test_func")
                self.assertEqual(args[1:], ("test_func", False, {}, [], False))
                self.workflow.functions["test_func"] = registered_func

                # Call test_func with a payload
                response = test_func(
                    {
                        "payload": 2,
                        "workflow_placement_decision": {
                            "run_id": "123",
                            "time_key": "1",
                            "workflow_placement": {
                                "current_deployment": {
                                    "instances": {
                                        "1": {
                                            "test_func": {
                                                "provider_region": {"provider": "provider1", "region": "region"},
                                                "identifier": "test_identifier",
                                            },
                                            "test-workflow-0_0_1-test_func::": {
                                                "provider_region": {"provider": "provider1", "region": "region"},
                                                "identifier": "test_identifier",
                                            },
                                        }
                                    }
                                }
                            },
                            "current_instance_name": "test_func",
                            "instances": {
                                "test_func": {
                                    "instance_name": "test_func",
                                    "succeeding_instances": ["test-workflow-0_0_1-test_func::"],
                                }
                            },
                        },
                    }
                )

                mock_factory_class.get_remote_client.assert_called_once_with("provider1", "region")

                # Check if invoke_serverless_function was called with the correct arguments
                mock_remote_client.invoke_function.assert_called_once_with(
                    message='{"workflow_placement_decision": {"run_id": "123", "time_key": "1", "workflow_placement": {"current_deployment": {"instances": {"1": {"test_func": {"provider_region": {"provider": "provider1", "region": "region"}, "identifier": "test_identifier"}, "test-workflow-0_0_1-test_func::": {"provider_region": {"provider": "provider1", "region": "region"}, "identifier": "test_identifier"}}}}}, "current_instance_name": "test-workflow-0_0_1-test_func::", "instances": {"test_func": {"instance_name": "test_func", "succeeding_instances": ["test-workflow-0_0_1-test_func::"]}}}, "transmission_taint": "37a5262", "number_of_hops_from_client_request": 1, "payload": 2}',
                    identifier="test_identifier",
                    workflow_instance_id="123",
                    sync=False,
                    function_name="test-workflow-0_0_1-test_func::",
                    expected_counter=-1,
                    current_instance_name="test_func",
                    alternative_message='{"workflow_placement_decision": {"run_id": "123", "time_key": "1", "workflow_placement": {"current_deployment": {"instances": {"1": {"test_func": {"provider_region": {"provider": "provider1", "region": "region"}, "identifier": "test_identifier"}, "test-workflow-0_0_1-test_func::": {"provider_region": {"provider": "provider1", "region": "region"}, "identifier": "test_identifier"}}}}}, "current_instance_name": "test-workflow-0_0_1-test_func::", "instances": {"test_func": {"instance_name": "test_func", "succeeding_instances": ["test-workflow-0_0_1-test_func::"]}}}, "transmission_taint": "37a5262", "number_of_hops_from_client_request": 1}',
                )

                # Check if the response from invoke_serverless_function is correct
                self.assertEqual(response, "Some response")

    def test_invoke_serverless_function_with_sync_successor(self):
        self.workflow.register_function = Mock()
        mock_remote_client = Mock()

        mock_remote_client.invoke_function = Mock(return_value=(0.0, 0.0, True, 0.0, 0.0))

        mock_uuid = Mock()
        mock_uuid.hex = "37a5262"
        with patch("caribou.deployment.client.caribou_workflow.RemoteClientFactory") as mock_factory_class:
            mock_factory_class.get_remote_client.return_value = mock_remote_client

            with patch("uuid.uuid4", return_value=mock_uuid):

                @self.workflow.serverless_function(name="test_func")
                def test_func(payload: dict[str, Any]) -> dict[str, Any]:
                    # Call invoke_serverless_function from within test_func
                    self.workflow.invoke_serverless_function(sync_func, payload)

                    return "Some response"

                # Check if the function was registered correctly
                args, _ = self.workflow.register_function.call_args
                registered_func = args[0]
                registered_func.name = "test_func"
                self.assertEqual(registered_func.__name__, "test_func")
                self.assertEqual(args[1:], ("test_func", False, {}, [], False))
                self.workflow.functions["test_func"] = registered_func

                @self.workflow.serverless_function(name="sync_func")
                def sync_func(payload: dict[str, Any]) -> dict[str, Any]:
                    return "Some response"

                # Check if the function was registered correctly
                args, _ = self.workflow.register_function.call_args
                registered_func = args[0]
                registered_func.name = "sync_func"
                self.assertEqual(registered_func.__name__, "sync_func")
                self.assertEqual(args[1:], ("sync_func", False, {}, [], False))
                self.workflow.functions["sync_func"] = registered_func

                message = {
                    "payload": 2,
                    "workflow_placement_decision": {
                        "run_id": "123",
                        "time_key": "1",
                        "workflow_placement": {
                            "current_deployment": {
                                "instances": {
                                    "1": {
                                        "test_func": {
                                            "provider_region": {"provider": "provider1", "region": "region"},
                                            "identifier": "test_identifier",
                                        },
                                        "test-workflow-0_0_1-sync_func:sync:": {
                                            "provider_region": {"provider": "provider1", "region": "region"},
                                            "identifier": "test_identifier",
                                        },
                                    }
                                }
                            }
                        },
                        "current_instance_name": "test_func",
                        "instances": {
                            "test_func": {
                                "instance_name": "test_func",
                                "succeeding_instances": ["test-workflow-0_0_1-sync_func:sync:"],
                            },
                            "test-workflow-0_0_1-sync_func:sync:": {
                                "instance_name": "test-workflow-0_0_1-sync_func:sync:",
                                "preceding_instances": ["test_func"],
                            },
                        },
                    },
                }

                # Call test_func with a payload
                response = test_func(message)

                mock_factory_class.get_remote_client.assert_called_once_with("provider1", "region")

                # Check if invoke_serverless_function was called with the correct arguments
                mock_remote_client.invoke_function.assert_called_once_with(
                    message='{"workflow_placement_decision": {"run_id": "123", "time_key": "1", "workflow_placement": {"current_deployment": {"instances": {"1": {"test_func": {"provider_region": {"provider": "provider1", "region": "region"}, "identifier": "test_identifier"}, "test-workflow-0_0_1-sync_func:sync:": {"provider_region": {"provider": "provider1", "region": "region"}, "identifier": "test_identifier"}}}}}, "current_instance_name": "test-workflow-0_0_1-sync_func:sync:", "instances": {"test_func": {"instance_name": "test_func", "succeeding_instances": ["test-workflow-0_0_1-sync_func:sync:"]}, "test-workflow-0_0_1-sync_func:sync:": {"instance_name": "test-workflow-0_0_1-sync_func:sync:", "preceding_instances": ["test_func"]}}}, "transmission_taint": "37a5262", "number_of_hops_from_client_request": 1, "payload": 2}',
                    identifier="test_identifier",
                    workflow_instance_id="123",
                    sync=True,
                    function_name="test-workflow-0_0_1-sync_func:sync:",
                    expected_counter=1,
                    current_instance_name="test_func",
                    alternative_message='{"workflow_placement_decision": {"run_id": "123", "time_key": "1", "workflow_placement": {"current_deployment": {"instances": {"1": {"test_func": {"provider_region": {"provider": "provider1", "region": "region"}, "identifier": "test_identifier"}, "test-workflow-0_0_1-sync_func:sync:": {"provider_region": {"provider": "provider1", "region": "region"}, "identifier": "test_identifier"}}}}}, "current_instance_name": "test-workflow-0_0_1-sync_func:sync:", "instances": {"test_func": {"instance_name": "test_func", "succeeding_instances": ["test-workflow-0_0_1-sync_func:sync:"]}, "test-workflow-0_0_1-sync_func:sync:": {"instance_name": "test-workflow-0_0_1-sync_func:sync:", "preceding_instances": ["test_func"]}}}, "transmission_taint": "37a5262", "number_of_hops_from_client_request": 1}',
                )

                # Check if the response from invoke_serverless_function is correct
                self.assertEqual(response, "Some response")

    def test_invoke_serverless_function_with_multiple_sync_successor(self):
        self.workflow.register_function = Mock()
        mock_remote_client = Mock()

        mock_remote_client.invoke_function = Mock(return_value=(0.0, 0.0, True, 0.0, 0.0))

        mock_uuid = Mock()
        mock_uuid.hex = "37a5262"
        with patch("caribou.deployment.client.caribou_workflow.RemoteClientFactory") as mock_factory_class:
            mock_factory_class.get_remote_client.return_value = mock_remote_client

            with patch("uuid.uuid4", return_value=mock_uuid):

                @self.workflow.serverless_function(name="test_func")
                def test_func(payload: dict[str, Any]) -> dict[str, Any]:
                    # Call invoke_serverless_function from within test_func
                    self.workflow.invoke_serverless_function(sync_func, payload)

                    return "Some response"

                # Check if the function was registered correctly
                args, _ = self.workflow.register_function.call_args
                registered_func = args[0]
                registered_func.name = "test_func"
                self.assertEqual(registered_func.__name__, "test_func")
                self.assertEqual(args[1:], ("test_func", False, {}, [], False))
                self.workflow.functions["test_func"] = registered_func

                @self.workflow.serverless_function(name="test_func2")
                def test_func2(payload: dict[str, Any]) -> dict[str, Any]:
                    # Call invoke_serverless_function from within test_func
                    self.workflow.invoke_serverless_function(sync_func, payload)

                    return "Some response"

                # Check if the function was registered correctly
                args, _ = self.workflow.register_function.call_args
                registered_func = args[0]
                registered_func.name = "test_func2"
                self.assertEqual(registered_func.__name__, "test_func2")
                self.assertEqual(args[1:], ("test_func2", False, {}, [], False))
                self.workflow.functions["test_func2"] = registered_func

                @self.workflow.serverless_function(name="sync_func")
                def sync_func(payload: dict[str, Any]) -> dict[str, Any]:
                    return "Some response"

                # Check if the function was registered correctly
                args, _ = self.workflow.register_function.call_args
                registered_func = args[0]
                registered_func.name = "sync_func"
                self.assertEqual(registered_func.__name__, "sync_func")
                self.assertEqual(args[1:], ("sync_func", False, {}, [], False))
                self.workflow.functions["sync_func"] = registered_func

                # Call test_func with a payload
                response = test_func(
                    {
                        "payload": 2,
                        "workflow_placement_decision": {
                            "run_id": "123",
                            "time_key": "1",
                            "workflow_placement": {
                                "current_deployment": {
                                    "instances": {
                                        "1": {
                                            "test_func": {
                                                "provider_region": {"provider": "provider1", "region": "region"},
                                                "identifier": "test_identifier",
                                            },
                                            "test_func2": {
                                                "provider_region": {"provider": "provider1", "region": "region"},
                                                "identifier": "test_identifier",
                                            },
                                            "test-workflow-0_0_1-sync_func:sync:": {
                                                "provider_region": {"provider": "provider1", "region": "region"},
                                                "identifier": "test_identifier",
                                            },
                                        }
                                    }
                                }
                            },
                            "current_instance_name": "test_func",
                            "instances": {
                                "test_func": {
                                    "instance_name": "test_func",
                                    "succeeding_instances": ["test-workflow-0_0_1-sync_func:sync:"],
                                },
                                "test_func2": {
                                    "instance_name": "test_func2",
                                    "succeeding_instances": ["test-workflow-0_0_1-sync_func:sync:"],
                                },
                                "test-workflow-0_0_1-sync_func:sync:": {
                                    "instance_name": "test-workflow-0_0_1-sync_func:sync:",
                                    "preceding_instances": ["test_func", "test_func2"],
                                },
                            },
                        },
                    }
                )

                mock_factory_class.get_remote_client.assert_called_once_with("provider1", "region")

                # Check if invoke_serverless_function was called with the correct arguments
                mock_remote_client.invoke_function.assert_called_once_with(
                    message='{"workflow_placement_decision": {"run_id": "123", "time_key": "1", "workflow_placement": {"current_deployment": {"instances": {"1": {"test_func": {"provider_region": {"provider": "provider1", "region": "region"}, "identifier": "test_identifier"}, "test_func2": {"provider_region": {"provider": "provider1", "region": "region"}, "identifier": "test_identifier"}, "test-workflow-0_0_1-sync_func:sync:": {"provider_region": {"provider": "provider1", "region": "region"}, "identifier": "test_identifier"}}}}}, "current_instance_name": "test-workflow-0_0_1-sync_func:sync:", "instances": {"test_func": {"instance_name": "test_func", "succeeding_instances": ["test-workflow-0_0_1-sync_func:sync:"]}, "test_func2": {"instance_name": "test_func2", "succeeding_instances": ["test-workflow-0_0_1-sync_func:sync:"]}, "test-workflow-0_0_1-sync_func:sync:": {"instance_name": "test-workflow-0_0_1-sync_func:sync:", "preceding_instances": ["test_func", "test_func2"]}}}, "transmission_taint": "37a5262", "number_of_hops_from_client_request": 1, "payload": 2}',
                    identifier="test_identifier",
                    workflow_instance_id="123",
                    sync=True,
                    function_name="test-workflow-0_0_1-sync_func:sync:",
                    expected_counter=2,
                    current_instance_name="test_func",
                    alternative_message='{"workflow_placement_decision": {"run_id": "123", "time_key": "1", "workflow_placement": {"current_deployment": {"instances": {"1": {"test_func": {"provider_region": {"provider": "provider1", "region": "region"}, "identifier": "test_identifier"}, "test_func2": {"provider_region": {"provider": "provider1", "region": "region"}, "identifier": "test_identifier"}, "test-workflow-0_0_1-sync_func:sync:": {"provider_region": {"provider": "provider1", "region": "region"}, "identifier": "test_identifier"}}}}}, "current_instance_name": "test-workflow-0_0_1-sync_func:sync:", "instances": {"test_func": {"instance_name": "test_func", "succeeding_instances": ["test-workflow-0_0_1-sync_func:sync:"]}, "test_func2": {"instance_name": "test_func2", "succeeding_instances": ["test-workflow-0_0_1-sync_func:sync:"]}, "test-workflow-0_0_1-sync_func:sync:": {"instance_name": "test-workflow-0_0_1-sync_func:sync:", "preceding_instances": ["test_func", "test_func2"]}}}, "transmission_taint": "37a5262", "number_of_hops_from_client_request": 1}',
                )

                # Check if the response from invoke_serverless_function is correct
                self.assertEqual(response, "Some response")

    def test_invoke_serverless_function_no_payload(self):
        self.workflow.register_function = Mock()
        mock_remote_client = Mock()

        mock_remote_client.invoke_function = Mock(return_value=(0.0, 0.0, True, 0.0, 0.0))

        mock_uuid = Mock()
        mock_uuid.hex = "37a5262"
        with patch("caribou.deployment.client.caribou_workflow.RemoteClientFactory") as mock_factory_class:
            mock_factory_class.get_remote_client.return_value = mock_remote_client

            with patch("uuid.uuid4", return_value=mock_uuid):

                @self.workflow.serverless_function(name="test_func")
                def test_func(event: dict[str, Any]) -> dict[str, Any]:
                    # Call invoke_serverless_function from within test_func
                    self.workflow.invoke_serverless_function(test_func)

                    return "Some response"

                # Check if the function was registered correctly
                args, _ = self.workflow.register_function.call_args
                registered_func = args[0]
                registered_func.name = "test_func"
                self.assertEqual(registered_func.__name__, "test_func")
                self.assertEqual(args[1:], ("test_func", False, {}, [], False))
                self.workflow.functions["test_func"] = registered_func

                # Call test_func with a payload
                response = test_func(
                    {
                        "workflow_placement_decision": {
                            "run_id": "123",
                            "time_key": "1",
                            "workflow_placement": {
                                "current_deployment": {
                                    "instances": {
                                        "1": {
                                            "test_func": {
                                                "provider_region": {"provider": "provider1", "region": "region"},
                                                "identifier": "test_identifier",
                                            },
                                            "test-workflow-0_0_1-test_func::": {
                                                "provider_region": {"provider": "provider1", "region": "region"},
                                                "identifier": "test_identifier",
                                            },
                                        }
                                    }
                                }
                            },
                            "current_instance_name": "test_func",
                            "instances": {
                                "test_func": {
                                    "instance_name": "test_func",
                                    "succeeding_instances": ["test-workflow-0_0_1-test_func::"],
                                }
                            },
                        }
                    }
                )

                mock_factory_class.get_remote_client.assert_called_once_with("provider1", "region")

                # Check if invoke_serverless_function was called with the correct arguments
                mock_remote_client.invoke_function.assert_called_once_with(
                    message='{"workflow_placement_decision": {"run_id": "123", "time_key": "1", "workflow_placement": {"current_deployment": {"instances": {"1": {"test_func": {"provider_region": {"provider": "provider1", "region": "region"}, "identifier": "test_identifier"}, "test-workflow-0_0_1-test_func::": {"provider_region": {"provider": "provider1", "region": "region"}, "identifier": "test_identifier"}}}}}, "current_instance_name": "test-workflow-0_0_1-test_func::", "instances": {"test_func": {"instance_name": "test_func", "succeeding_instances": ["test-workflow-0_0_1-test_func::"]}}}, "transmission_taint": "37a5262", "number_of_hops_from_client_request": 1}',
                    identifier="test_identifier",
                    workflow_instance_id="123",
                    sync=False,
                    function_name="test-workflow-0_0_1-test_func::",
                    expected_counter=-1,
                    current_instance_name="test_func",
                    alternative_message=None,
                )

                # Check if the response from invoke_serverless_function is correct
                self.assertEqual(response, "Some response")

    def test_invoke_serverless_function_conditional_false(self):
        self.workflow.register_function = Mock()

        self.workflow._inform_sync_node_of_conditional_non_execution = Mock(return_value=(0.0, 0.0, []))

        self.workflow.get_successor_workflow_placement_decision = Mock(return_value=("provider1", "region", None))

        @self.workflow.serverless_function(name="test_func")
        def test_func(payload: str) -> dict[str, Any]:
            self.workflow.invoke_serverless_function(test_func, payload, False)

        # Check if the function was registered correctly
        args, _ = self.workflow.register_function.call_args
        registered_func = args[0]
        registered_func.name = "test_func"
        self.assertEqual(registered_func.__name__, "test_func")
        self.assertEqual(args[1:], ("test_func", False, {}, [], False))
        self.workflow.functions["test_func"] = registered_func

        test_func(
            {
                "payload": '{"key": "value"}',
                "workflow_placement_decision": {
                    "run_id": "123",
                    "workflow_placement": {
                        "current_deployment": {
                            "instances": {
                                "test_func": {
                                    "provider_region": {"provider": "provider1", "region": "region"},
                                    "identifier": "test_identifier",
                                },
                                "test-workflow-0_0_1-test_func": {
                                    "provider_region": {"provider": "provider1", "region": "region"},
                                    "identifier": "test_identifier",
                                },
                            }
                        }
                    },
                    "current_instance_name": "test_func",
                    "instances": {
                        "some_instance": {
                            "instance_name": "some_instance",
                            "succeeding_instances": ["test-workflow-0_0_1-test_func::"],
                        },
                        "test_func": {
                            "instance_name": "test_func",
                            "succeeding_instances": ["test-workflow-0_0_1-test_func::"],
                        },
                    },
                },
            }
        )

        self.workflow._inform_sync_node_of_conditional_non_execution.assert_called_once_with(
            {
                "run_id": "123",
                "workflow_placement": {
                    "current_deployment": {
                        "instances": {
                            "test_func": {
                                "provider_region": {"provider": "provider1", "region": "region"},
                                "identifier": "test_identifier",
                            },
                            "test-workflow-0_0_1-test_func": {
                                "provider_region": {"provider": "provider1", "region": "region"},
                                "identifier": "test_identifier",
                            },
                        }
                    }
                },
                "current_instance_name": "test_func",
                "instances": {
                    "some_instance": {
                        "instance_name": "some_instance",
                        "succeeding_instances": ["test-workflow-0_0_1-test_func::"],
                    },
                    "test_func": {
                        "instance_name": "test_func",
                        "succeeding_instances": ["test-workflow-0_0_1-test_func::"],
                    },
                },
            },
            "test-workflow-0_0_1-test_func::",
            "test_func",
        )

    # TODO: Look more into this function
    def test_invoke_serverless_function_json_argument(self):
        self.workflow.register_function = Mock()
        mock_remote_client = Mock()

        mock_remote_client.invoke_function = Mock(return_value=(0.0, 0.0, True, 0.0, 0.0))

        mock_uuid = Mock()
        mock_uuid.hex = "37a5262"
        with patch("caribou.deployment.client.caribou_workflow.RemoteClientFactory") as mock_factory_class:
            mock_factory_class.get_remote_client.return_value = mock_remote_client

            with patch("uuid.uuid4", return_value=mock_uuid):

                @self.workflow.serverless_function(name="test_func")
                def test_func(payload: str) -> dict[str, Any]:
                    # Call invoke_serverless_function from within test_func
                    self.workflow.invoke_serverless_function(test_func, payload)

                    return "Some response"

                # Check if the function was registered correctly
                args, _ = self.workflow.register_function.call_args
                registered_func = args[0]
                registered_func.name = "test_func"
                self.assertEqual(registered_func.__name__, "test_func")
                self.assertEqual(args[1:], ("test_func", False, {}, [], False))
                self.workflow.functions["test_func"] = registered_func

                # Call test_func with a payload
                response = test_func(
                    {
                        "payload": '{"key": "value"}',
                        "workflow_placement_decision": {
                            "run_id": "123",
                            "time_key": "1",
                            "workflow_placement": {
                                "current_deployment": {
                                    "instances": {
                                        "1": {
                                            "test_func": {
                                                "provider_region": {"provider": "provider1", "region": "region"},
                                                "identifier": "test_identifier",
                                            },
                                            "test-workflow-0_0_1-test_func::": {
                                                "provider_region": {"provider": "provider1", "region": "region"},
                                                "identifier": "test_identifier",
                                            },
                                        }
                                    }
                                }
                            },
                            "current_instance_name": "test_func",
                            "instances": {
                                "some_instance": {
                                    "instance_name": "some_instance",
                                    "succeeding_instances": ["test-workflow-0_0_1-test_func::"],
                                },
                                "test_func": {
                                    "test_func": "instance_name",
                                    "succeeding_instances": ["test-workflow-0_0_1-test_func::"],
                                },
                            },
                        },
                    }
                )

                mock_factory_class.get_remote_client.assert_called_once_with("provider1", "region")

                # Check if invoke_serverless_function was called with the correct arguments
                mock_remote_client.invoke_function.assert_called_once_with(
                    message='{"workflow_placement_decision": {"run_id": "123", "time_key": "1", "workflow_placement": {"current_deployment": {"instances": {"1": {"test_func": {"provider_region": {"provider": "provider1", "region": "region"}, "identifier": "test_identifier"}, "test-workflow-0_0_1-test_func::": {"provider_region": {"provider": "provider1", "region": "region"}, "identifier": "test_identifier"}}}}}, "current_instance_name": "test-workflow-0_0_1-test_func::", "instances": {"some_instance": {"instance_name": "some_instance", "succeeding_instances": ["test-workflow-0_0_1-test_func::"]}, "test_func": {"test_func": "instance_name", "succeeding_instances": ["test-workflow-0_0_1-test_func::"]}}}, "transmission_taint": "37a5262", "number_of_hops_from_client_request": 1, "payload": "{\\"key\\": \\"value\\"}"}',
                    identifier="test_identifier",
                    workflow_instance_id="123",
                    sync=False,
                    function_name="test-workflow-0_0_1-test_func::",
                    expected_counter=-1,
                    current_instance_name="test_func",
                    alternative_message='{"workflow_placement_decision": {"run_id": "123", "time_key": "1", "workflow_placement": {"current_deployment": {"instances": {"1": {"test_func": {"provider_region": {"provider": "provider1", "region": "region"}, "identifier": "test_identifier"}, "test-workflow-0_0_1-test_func::": {"provider_region": {"provider": "provider1", "region": "region"}, "identifier": "test_identifier"}}}}}, "current_instance_name": "test-workflow-0_0_1-test_func::", "instances": {"some_instance": {"instance_name": "some_instance", "succeeding_instances": ["test-workflow-0_0_1-test_func::"]}, "test_func": {"test_func": "instance_name", "succeeding_instances": ["test-workflow-0_0_1-test_func::"]}}}, "transmission_taint": "37a5262", "number_of_hops_from_client_request": 1}',
                )

                # Check if the response from invoke_serverless_function is correct
                self.assertEqual(response, "Some response")

    def test_get_successors(self):
        def test_function(x):
            return x

        name = "test_function"
        entry_point = True
        regions_and_providers = {}
        environment_variables = []

        function_obj_1 = CaribouFunction(test_function, name, entry_point, regions_and_providers, environment_variables)

        self.workflow.functions = {"test_function": function_obj_1}

        self.assertEqual(self.workflow.get_successors(function_obj_1), [])

        def test_function_2(x):
            return self.workflow.invoke_serverless_function(test_function, x)

        function_obj_2 = CaribouFunction(
            test_function_2, name, entry_point, regions_and_providers, environment_variables
        )

        self.workflow.functions = {"test_function": function_obj_1, "test_function_2": function_obj_2}

        self.assertEqual(self.workflow.get_successors(function_obj_2), [function_obj_1])

        def function(x):
            return self.workflow.invoke_serverless_function(test_function_2)

        function_obj_3 = CaribouFunction(function, name, entry_point, regions_and_providers, environment_variables)

        self.assertEqual(self.workflow.get_successors(function_obj_3), [function_obj_2])

        def function(x):
            self.workflow.invoke_serverless_function(test_function, {"payload": x})
            self.workflow.invoke_serverless_function(test_function_2, x)

        function_obj_4 = CaribouFunction(function, name, entry_point, regions_and_providers, environment_variables)

        self.assertEqual(self.workflow.get_successors(function_obj_4), [function_obj_1, function_obj_2])

        def not_registered_function(x):
            return x

        def function(x):
            self.workflow.invoke_serverless_function(test_function, x)
            self.workflow.invoke_serverless_function(not_registered_function, x)

        function_obj_5 = CaribouFunction(function, name, entry_point, regions_and_providers, environment_variables)

        with self.assertRaises(
            RuntimeError, msg="Could not find function with name not_registered_function, was the function registered?"
        ):
            self.workflow.get_successors(function_obj_5)

    def test_get_workflow_placement_decision(self):
        self.workflow._current_workflow_placement_decision = "decision"
        self.assertEqual(self.workflow.get_workflow_placement_decision(), "decision")

    def test_get_next_instance_name(self):
        workflow_placement_decision = {
            "run_id": "123",
            "instances": {
                "current_instance": {
                    "instance_name": "current_instance",
                    "succeeding_instances": [
                        f"{self.workflow.name}-{self.workflow.version.replace('.', '_')}-successor_function:sync:",
                        f"{self.workflow.name}-{self.workflow.version.replace('.', '_')}-successor_function:0_1",
                    ],
                },
                "other_instance": {
                    "instance_name": "other_instance",
                    "succeeding_instances": [
                        f"{self.workflow.name}-{self.workflow.version.replace('.', '_')}-other_successor_function:sync:"
                    ],
                },
            },
        }
        current_instance_name = "current_instance"
        successor_function_name = "successor_function"
        self.workflow._run_id_to_successor_index = {"123": -1}

        next_instance_name = self.workflow.get_next_instance_name(
            current_instance_name, workflow_placement_decision, successor_function_name
        )

        self.assertEqual(
            next_instance_name,
            f"{self.workflow.name}-{self.workflow.version.replace('.', '_')}-successor_function:sync:",
        )

    def test_get_next_instance_name_non_sync_successor(self):
        workflow_placement_decision = {
            "run_id": "123",
            "instances": {
                "current_instance": {
                    "instance_name": "current_instance",
                    "succeeding_instances": [
                        f"{self.workflow.name}-{self.workflow.version.replace('.', '_')}-successor_function:current_instance_0_0",
                        f"{self.workflow.name}-{self.workflow.version.replace('.', '_')}-successor_function:current_instance_0_1",
                    ],
                },
                "other_instance": {
                    "instance_name": "other_instance",
                    "succeeding_instances": [
                        f"{self.workflow.name}-{self.workflow.version.replace('.', '_')}-other_successor_function:sync:"
                    ],
                },
            },
        }
        current_instance_name = "current_instance"
        successor_function_name = "successor_function"

        # Test with _successor_index = 0
        self.workflow._run_id_to_successor_index = {"123": 0}
        next_instance_name = self.workflow.get_next_instance_name(
            current_instance_name, workflow_placement_decision, successor_function_name
        )
        self.assertEqual(
            next_instance_name,
            f"{self.workflow.name}-{self.workflow.version.replace('.', '_')}-successor_function:current_instance_0_0",
        )

        # The _successor_index should be incremented
        next_instance_name = self.workflow.get_next_instance_name(
            current_instance_name, workflow_placement_decision, successor_function_name
        )
        self.assertEqual(
            next_instance_name,
            f"{self.workflow.name}-{self.workflow.version.replace('.', '_')}-successor_function:current_instance_0_1",
        )

    def test_get_next_instance_name_no_current_instance(self):
        workflow_placement_decision = {
            "instances": [
                {"instance_name": "other_instance", "succeeding_instances": ["other_successor_function:sync:"]},
            ]
        }
        current_instance_name = "current_instance"
        successor_function_name = "successor_function"

        with self.assertRaises(RuntimeError, msg="Could not find current instance"):
            self.workflow.get_next_instance_name(
                current_instance_name, workflow_placement_decision, successor_function_name
            )

    def test_get_next_instance_name_no_successor_instance(self):
        workflow_placement_decision = {
            "instances": [
                {"instance_name": "current_instance", "succeeding_instances": ["other_successor_function:sync:"]},
            ]
        }
        current_instance_name = "current_instance"
        successor_function_name = "successor_function"

        with self.assertRaises(RuntimeError, msg="Could not find successor instance"):
            self.workflow.get_next_instance_name(
                current_instance_name, workflow_placement_decision, successor_function_name
            )

    def test_get_next_instance_name_multiple_successor_instances_no_match(self):
        workflow_placement_decision = {
            "instances": [
                {
                    "instance_name": "current_instance",
                    "succeeding_instances": ["other_successor_function:sync:", "other_successor_function:0_1"],
                },
            ]
        }
        current_instance_name = "current_instance"
        successor_function_name = "successor_function"

        with self.assertRaises(RuntimeError, msg="Could not find successor instance"):
            self.workflow.get_next_instance_name(
                current_instance_name, workflow_placement_decision, successor_function_name
            )

    def test_register_function(self):
        function = lambda x: x
        name = "test_function"
        entry_point = True
        regions_and_providers = {"region1": "provider1"}
        environment_variables = [{"key": "value"}]
        allow_placement_decision_override = False

        self.workflow.register_function(
            function, name, entry_point, regions_and_providers, environment_variables, allow_placement_decision_override
        )

        self.assertIn(name, self.workflow._function_names)
        self.assertIn(function.__name__, self.workflow.functions)
        self.assertIsInstance(self.workflow.functions[function.__name__], CaribouFunction)

    def test_register_function_duplicate_function_name(self):
        function = lambda x: x
        name = "test_function"
        entry_point = True
        regions_and_providers = {"region1": "provider1"}
        environment_variables = [{"key": "value"}]
        allow_placement_decision_override = False

        self.workflow.register_function(
            function, name, entry_point, regions_and_providers, environment_variables, allow_placement_decision_override
        )

        with self.assertRaises(RuntimeError, msg=f"Function with function name {function.__name__} already registered"):
            self.workflow.register_function(
                function,
                name,
                entry_point,
                regions_and_providers,
                environment_variables,
                allow_placement_decision_override,
            )

    def test_register_function_duplicate_name(self):
        def function1(x):
            return x

        def function2(y):
            return y

        name = "test_function"
        entry_point = True
        regions_and_providers = {"region1": "provider1"}
        environment_variables = [{"key": "value"}]
        allow_placement_decision_override = False

        self.workflow.register_function(
            function1,
            name,
            entry_point,
            regions_and_providers,
            environment_variables,
            allow_placement_decision_override,
        )

        with self.assertRaises(RuntimeError, msg=f"Function with given name {name} already registered"):
            self.workflow.register_function(
                function2,
                name,
                entry_point,
                regions_and_providers,
                environment_variables,
                allow_placement_decision_override,
            )

    def test_get_predecessor_data(self):
        self.workflow.get_current_instance_provider_region_instance_name = Mock(
            return_value=("provider1", "region1", "current_instance", "workflow_instance_id")
        )
        self.workflow.get_run_id = Mock(return_value="workflow_instance_id")
        client_mock = Mock()
        client_mock.get_predecessor_data.return_value = (['{"key": "value"}'], 0.0)
        with patch(
            "caribou.common.models.remote_client.remote_client_factory.RemoteClientFactory.get_remote_client",
            return_value=client_mock,
        ):
            result = self.workflow.get_predecessor_data()

            self.assertEqual(result, [{"key": "value"}])

    @patch("caribou.deployment.client.caribou_workflow.datetime")
    def test_get_current_instance_provider_region_instance_name(self, mock_datetime):
        wrapper_frame = Mock()
        self.workflow.get_wrapper_frame = Mock(return_value=(None, wrapper_frame))
        self.workflow.get_workflow_placement_decision = Mock(
            return_value={
                "current_instance_name": "current_instance",
                "run_id": "workflow_instance_id",
                "time_key": "1",
                "send_to_home_region": False,
                "workflow_placement": {
                    "current_deployment": {
                        "instances": {
                            "1": {
                                "current_instance": {"provider_region": {"provider": "provider1", "region": "region1"}}
                            }
                        },
                        "expiry_time": "2022-01-01 00:01:00",
                    },
                },
            }
        )

        mock_datetime.now.return_value = datetime(2022, 1, 1, 0, 0, 0)
        mock_datetime.strptime.return_value = datetime(2022, 1, 1, 0, 1, 0)

        result = self.workflow.get_current_instance_provider_region_instance_name()

        self.assertEqual(result, ("provider1", "region", "test_func", "123"))

    def test_inform_sync_node_of_conditional_non_execution(self):
        mock_remote_client = Mock()
        mock_remote_client_factory = Mock()
        mock_remote_client_factory.get_remote_client = Mock(return_value=mock_remote_client)

        workflow_placement_decision = {
            "current_instance_name": "not_called_instance",
            "run_id": "workflow_instance_id",
            "workflow_placement": {
                "not_called_instance": {"provider_region": {"provider": "provider1", "region": "region1"}},
                "next_instance": {"provider_region": {"provider": "provider1", "region": "region1"}},
                "sync_node": {"provider_region": {"provider": "provider1", "region": "region1"}},
            },
            "instances": {
                "not_called_instance:sync:": {
                    "instance_name": "not_called_instance",
                    "succeeding_instances": ["next_instance"],
                    "dependent_sync_predecessors": [["next_instance", "sync_node"]],
                },
                "next_instance": {
                    "instance_name": "next_instance",
                    "preceding_instances": ["not_called_instance"],
                    "successor_sync_node": "sync_node",
                },
                "sync_node": {"instance_name": "sync_node", "preceding_instances": ["next_instance"]},
            },
        }

        with patch(
            "caribou.deployment.client.caribou_workflow.RemoteClientFactory",
            return_value=mock_remote_client_factory,
        ) as mock_factory_class:
            mock_factory_class.get_remote_client("provider1", "region1").set_predecessor_reached.return_value = (
                [True],
                0.0,
                0.0,
            )
            mock_factory_class.get_remote_client("provider1", "region1").invoke_function.return_value = None
            self.workflow.get_successor_workflow_placement_decision = Mock(
                return_value=("provider1", "region1", "identifier")
            )
            self.workflow.get_successor_workflow_placement_decision_dictionary = Mock(
                return_value=workflow_placement_decision
            )

            self.workflow._inform_sync_node_of_conditional_non_execution(
                workflow_placement_decision, "not_called_instance:sync:", "current_node::", "next_instance"
            )

            mock_factory_class.get_remote_client.assert_called_with("provider1", "region1")
            mock_factory_class.get_remote_client(
                "provider1", "region1"
            ).set_predecessor_reached.assert_called_once_with(
                predecessor_name="current_node::",
                sync_node_name="not_called_instance:sync:",
                workflow_instance_id="workflow_instance_id",
                direct_call=False,
            )

    def test_inform_sync_node_of_conditional_non_execution_no_sync(self):
        mock_remote_client = Mock()
        mock_remote_client_factory = Mock()
        mock_remote_client_factory.get_remote_client = Mock(return_value=mock_remote_client)

        workflow_placement_decision = {
            "current_instance_name": "not_called_instance",
            "run_id": "workflow_instance_id",
            "workflow_placement": {
                "not_called_instance": {"provider_region": {"provider": "provider1", "region": "region1"}},
                "next_instance": {"provider_region": {"provider": "provider1", "region": "region1"}},
            },
            "instances": {
                "not_called_instance::": {
                    "instance_name": "not_called_instance",
                    "succeeding_instances": ["next_instance"],
                },
                "next_instance": {
                    "instance_name": "next_instance",
                    "preceding_instances": ["not_called_instance"],
                },
            },
        }

        with patch(
            "caribou.deployment.client.caribou_workflow.RemoteClientFactory",
            return_value=mock_remote_client_factory,
        ) as mock_factory_class:
            self.workflow._inform_sync_node_of_conditional_non_execution(
                workflow_placement_decision, "not_called_instance::", "current_node::", "next_instance"
            )

            mock_factory_class.get_remote_client.assert_not_called()

    @patch("caribou.deployment.client.caribou_workflow.datetime")
    def test_get_deployment_key(self, mock_datetime):
        # Arrange
        workflow_placement_decision = {
            "workflow_placement": {"current_deployment": {"expiry_time": "2022-01-01 00:00:00"}},
            "send_to_home_region": False,
        }
        mock_datetime.now.return_value = datetime(2022, 1, 1, 0, 1, 0)
        mock_datetime.strptime.return_value = datetime(2022, 1, 1, 0, 0, 0)

        # Act & Assert
        self.assertEqual(self.workflow._get_deployment_key(workflow_placement_decision), "home_deployment")
        workflow_placement_decision["send_to_home_region"] = True
        self.assertEqual(self.workflow._get_deployment_key(workflow_placement_decision), "home_deployment")

        # Change the current time to before the expiry time
        mock_datetime.now.return_value = datetime(2021, 12, 31, 23, 59, 59)
        workflow_placement_decision["send_to_home_region"] = False
        self.assertEqual(self.workflow._get_deployment_key(workflow_placement_decision), "current_deployment")

        # Test with no expiry time
        workflow_placement_decision["workflow_placement"]["current_deployment"]["expiry_time"] = None
        self.assertEqual(self.workflow._get_deployment_key(workflow_placement_decision), "home_deployment")

    @patch("caribou.deployment.client.caribou_workflow.datetime")
    def test_get_time_key(self, mock_datetime):
        # Arrange
        workflow_placement_decision = {
            "workflow_placement": {"current_deployment": {"time_keys": ["0", "6", "12", "18"]}},
            "time_key": "6",
        }
        mock_datetime.now.return_value = datetime(2022, 1, 1, 7, 0, 0)

        # Act & Assert
        self.assertEqual(self.workflow._get_time_key(workflow_placement_decision), "6")
        del workflow_placement_decision["time_key"]
        self.assertEqual(self.workflow._get_time_key(workflow_placement_decision), "6")

        # Change the current time to before the first time key
        mock_datetime.now.return_value = datetime(2022, 1, 1, 0, 0, 0)
        self.assertEqual(self.workflow._get_time_key(workflow_placement_decision), "0")

        # Test with no current_deployment
        del workflow_placement_decision["workflow_placement"]["current_deployment"]
        self.assertEqual(self.workflow._get_time_key(workflow_placement_decision), "N/A")

    def test_invoke_serverless_function_async(self):
        self.workflow.get_workflow_placement_decision = Mock(return_value={"current_instance_name": "test_instance"})
        future = Future()
        future.set_result(None)
        mock_worker = Mock(return_value=future)
        self.workflow._thread_pool.submit = mock_worker

        with patch.object(self.workflow, "get_successor_instance_name", return_value=("successor", {}, "next_function")):
            with patch.object(
                self.workflow,
                "get_successor_workflow_placement_decision",
                return_value=("provider", "region", "identifier"),
            ):
                self.workflow.invoke_serverless_function(lambda x: x, payload={"data": "value"})

        mock_worker.assert_called_once()

    def test_get_successor_workflow_placement_decision_home_region(self):
        workflow_placement_decision = {
            "send_to_home_region": True,
            "workflow_placement": {
                "home_deployment": {
                    "successor_instance_name": {
                        "provider_region": {"provider": "provider1", "region": "region1"},
                        "identifier": "identifier1",
                    }
                }
            },
        }

        provider, region, identifier = self.workflow.get_successor_workflow_placement_decision(
            "successor_instance_name", workflow_placement_decision
        )

        self.assertEqual(provider, "provider1")
        self.assertEqual(region, "region1")
        self.assertEqual(identifier, "identifier1")

    def test_get_successor_workflow_placement_decision_current_deployment(self):
        workflow_placement_decision = {
            "send_to_home_region": False,
            "workflow_placement": {
                "current_deployment": {
                    "instances": {
                        "current_time_key": {
                            "successor_instance_name": {
                                "provider_region": {"provider": "provider2", "region": "region2"},
                                "identifier": "identifier2",
                            }
                        }
                    }
                }
            },
            "time_key": "current_time_key",
        }

        provider, region, identifier = self.workflow.get_successor_workflow_placement_decision(
            "successor_instance_name", workflow_placement_decision
        )

        self.assertEqual(provider, "provider2")
        self.assertEqual(region, "region2")
        self.assertEqual(identifier, "identifier2")

    def test_log_for_retrieval(self):
        with patch("caribou.deployment.client.caribou_workflow.logger") as mock_logger:
            self.workflow.log_for_retrieval("Test message", "123")
            mock_logger.caribou.assert_called_once_with(
                "TIME (%s) RUN_ID (%s) MESSAGE (%s) LOG_VERSION (%s)",
                unittest.mock.ANY,
                "123",
                "Test message",
                LOG_VERSION,
            )

    def test_log_cpu_model(self):
        self.workflow.get_cpu_info = Mock(return_value="Intel(R) Xeon(R) CPU @ 2.30GHz")
        with patch.object(self.workflow, "log_for_retrieval") as mock_log:
            self.workflow._log_cpu_model({"run_id": "123", "current_instance_name": "test_func"}, from_redirector=True)
            mock_log.assert_called_once_with(
                "USED_CPU_MODEL: CPU_MODEL (Intel<R> Xeon<R> CPU @ 2.30GHz) used in INSTANCE (test_func) and from FROM_REDIRECTOR (True)",
                "123",
            )

    def test_get_time_key(self):
        workflow_placement_decision = {
            "workflow_placement": {"current_deployment": {"time_keys": ["0", "6", "12", "18"]}}
        }
        with patch("caribou.deployment.client.caribou_workflow.datetime") as mock_datetime:
            mock_datetime.now.return_value = datetime(2022, 1, 1, 7, 0, 0)
            result = self.workflow._get_time_key(workflow_placement_decision)
            self.assertEqual(result, "6")

    def test_get_predecessor_data(self):
        self.workflow.get_current_instance_provider_region_instance_name = Mock(
            return_value=("provider1", "region1", "test_func", "workflow_instance_id")
        )
        self.workflow.get_run_id = Mock(return_value="workflow_instance_id")
        mock_remote_client = Mock()
        mock_remote_client.get_predecessor_data.return_value = (['{"key": "value"}'], 0.0)

        with patch(
            "caribou.common.models.remote_client.remote_client_factory.RemoteClientFactory.get_remote_client",
            return_value=mock_remote_client,
        ):
            result = self.workflow.get_predecessor_data()

            self.assertEqual(result, [{"key": "value"}])

    def test_register_function_duplicate_function_name(self):
        function = lambda x: x
        name = "test_function"
        entry_point = True
        regions_and_providers = {"region1": "provider1"}
        environment_variables = [{"key": "value"}]
        allow_placement_decision_override = False

        self.workflow.register_function(
            function, name, entry_point, regions_and_providers, environment_variables, allow_placement_decision_override
        )

        with self.assertRaises(RuntimeError, msg=f"Function with function name {function.__name__} already registered"):
            self.workflow.register_function(
                function,
                name,
                entry_point,
                regions_and_providers,
                environment_variables,
                allow_placement_decision_override,
            )


class TestCustomEncoder(unittest.TestCase):
    def test_encode_bytes(self):
        encoder = CustomEncoder()
        data = {"key": b"test"}
        result = encoder.encode(data)
        expected = '{"key": "b64:dGVzdA=="}'
        self.assertEqual(result, expected)


class TestCustomDecoder(unittest.TestCase):
    def test_decode_bytes(self):
        decoder = CustomDecoder()
        data = '{"key": "b64:dGVzdA=="}'
        result = decoder.decode(data)
        expected = {"key": b"test"}
        self.assertEqual(result, expected)

    def test_decode_nested_bytes(self):
        decoder = CustomDecoder()
        data = '{"key": {"nested_key": "b64:dGVzdA=="}}'
        result = decoder.decode(data)
        expected = {"key": {"nested_key": b"test"}}
        self.assertEqual(result, expected)

    def test_decode_list_of_bytes(self):
        decoder = CustomDecoder()
        data = '{"key": ["b64:dGVzdA==", "b64:dGVzdA=="]}'
        result = decoder.decode(data)
        expected = {"key": [b"test", b"test"]}
        self.assertEqual(result, expected)

    def test_get_remote_client(self):
        workflow = CaribouWorkflow(name="test-workflow", version="0.0.1")

        # Mock the RemoteClientFactory.get_remote_client method
        with patch(
            "caribou.deployment.client.caribou_workflow.RemoteClientFactory.get_remote_client"
        ) as mock_get_remote_client:
            # Mock the return value of the get_remote_client method
            mock_remote_client = "mock_remote_client"
            mock_get_remote_client.return_value = mock_remote_client

            # Call the _get_remote_client method
            provider = "mock_provider"
            region = "mock_region"
            remote_client = workflow._get_remote_client(provider, region)

            # Assert that the RemoteClientFactory.get_remote_client method was called with the correct arguments
            mock_get_remote_client.assert_called_once_with(provider, region)

            # Assert that the returned remote_client is the same as the mocked remote_client
            self.assertEqual(remote_client, mock_remote_client)


if __name__ == "__main__":
    unittest.main()
