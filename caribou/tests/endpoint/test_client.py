import unittest
from datetime import datetime
from unittest.mock import Mock, patch, MagicMock
from caribou.common.models.remote_client.aws_remote_client import AWSRemoteClient
from caribou.endpoint.client import Client
import json
from caribou.common.models.endpoints import Endpoints
from unittest.mock import call
from caribou.common.models.remote_client.remote_client_factory import RemoteClientFactory
import botocore


class TestClient(unittest.TestCase):
    @patch("caribou.endpoint.client.datetime")
    @patch("caribou.deployment.client.caribou_workflow.RemoteClientFactory.get_remote_client")
    @patch.object(Endpoints, "get_deployment_algorithm_workflow_placement_decision_client")
    def test_successful_workflow_placement_decision_retrieval_and_invocation(
        self, mock_get_deployment_algorithm_workflow_placement_decision_client, mock_get_remote_client, mock_datetime
    ):
        mock_deployment_algorithm_client = MagicMock()
        mock_deployment_algorithm_client.get_value_from_table.return_value = (
            json.dumps(
                {
                    "current_instance_name": "instance1",
                    "workflow_placement": {
                        "current_deployment": {
                            "time_keys": ["0"],
                            "instances": {
                                "0": {
                                    "instance1": {
                                        "provider_region": {"provider": "aws", "region": "us-east-1"},
                                        "identifier": "function1",
                                    }
                                }
                            },
                            "expiry_time": "2022-01-01 00:01:00",
                        },
                        "home_deployment": {
                            "instances": {
                                "instance1": {
                                    "provider_region": {"provider": "aws", "region": "us-west-2"},
                                    "identifier": "function1",
                                }
                            }
                        },
                    },
                }
            ),
            0.0,
        )
        mock_get_deployment_algorithm_workflow_placement_decision_client.return_value = mock_deployment_algorithm_client

        # Mocking the remote client invocation
        mock_remote_client = MagicMock()
        mock_get_remote_client.return_value = mock_remote_client

        # Mock the current time
        mock_datetime.now.return_value = datetime(2022, 1, 1, 0, 0, 0)
        mock_datetime.strptime.return_value = datetime(2022, 1, 1, 0, 1, 0)

        mock_uuid = Mock()
        mock_uuid.hex = "37a5262"
        with patch("uuid.uuid4", return_value=mock_uuid):
            client = Client("workflow_name")
            client._home_region_threshold = 0.0  # Never send to home region
            client.run('{"key": "value"}')

            # Verify the remote client was invoked with the correct parameters
            mock_get_remote_client.assert_called_with("aws", "us-east-1")
            mock_remote_client.invoke_function.assert_called_once_with(
                message='{"payload": "{\\"key\\": \\"value\\"}", "time_request_sent": "2022-01-01 00:00:00,000000", "workflow_placement_decision": {"current_instance_name": "instance1", "workflow_placement": {"current_deployment": {"time_keys": ["0"], "instances": {"0": {"instance1": {"provider_region": {"provider": "aws", "region": "us-east-1"}, "identifier": "function1"}}}, "expiry_time": "2022-01-01 00:01:00"}, "home_deployment": {"instances": {"instance1": {"provider_region": {"provider": "aws", "region": "us-west-2"}, "identifier": "function1"}}}}, "time_key": "0", "send_to_home_region": false, "run_id": "37a5262", "data_size": 3.8370490074157715e-07, "consumed_read_capacity": 0.0}, "number_of_hops_from_client_request": 0, "permit_redirection": false, "redirected": false, "request_source": "Caribou CLI"}',
                identifier="function1",
            )

    @patch("caribou.endpoint.client.RemoteClientFactory.get_remote_client")
    def test_get_remote_client(self, mock_get_remote_client):
        # Create an instance of the Client class
        client = Client()

        # Clear mock_get_remote_client call count
        mock_get_remote_client.reset_mock()

        # Mock the return value of get_remote_client
        mock_remote_client = "mock_remote_client"
        mock_get_remote_client.return_value = mock_remote_client

        # Call the _get_remote_client method
        provider = "aws"
        region = "us-east-1"
        remote_client = client._get_remote_client(provider, region)

        # Assert that get_remote_client was called with the correct arguments
        mock_get_remote_client.assert_called_once_with(provider, region)

        # Assert that the returned remote_client is the same as the mocked remote_client
        self.assertEqual(remote_client, mock_remote_client)

    @patch.object(Endpoints, "get_deployment_algorithm_workflow_placement_decision_client")
    def test_no_workflow_placement_decision_found(
        self, mock_get_deployment_algorithm_workflow_placement_decision_client
    ):
        # Mocking the scenario where no workflow placement decision is found
        mock_deployment_algorithm_client = MagicMock()
        mock_deployment_algorithm_client.get_value_from_table.return_value = (None, 0.0)
        mock_get_deployment_algorithm_workflow_placement_decision_client.return_value = mock_deployment_algorithm_client

        client = Client("workflow_name")

        with self.assertRaises(RuntimeError) as context:
            client.run({"key": "value"})

        self.assertEqual(
            str(context.exception),
            "No workflow placement decision found for workflow, did you deploy the workflow and is the workflow id (workflow_name) correct?",
        )

    @patch.object(Endpoints, "get_deployment_manager_client")
    def test_list_workflows_no_workflows_deployed(self, mock_get_deployment_manager_client):
        # Mocking the scenario where no workflows are deployed
        mock_deployment_algorithm_client = MagicMock()
        mock_deployment_algorithm_client.get_keys.return_value = None
        mock_get_deployment_manager_client.return_value = mock_deployment_algorithm_client

        client = Client()

        # Capture the output of the print statements
        with patch("builtins.print") as mocked_print:
            client.list_workflows()

        # Check that the print statement in the if block was executed
        mocked_print.assert_called_once_with("No workflows deployed")

    @patch.object(Endpoints, "get_deployment_manager_client")
    def test_list_workflows_workflows_deployed(self, mock_get_deployment_manager_client):
        # Mocking the scenario where workflows are deployed
        mock_deployment_algorithm_client = MagicMock()
        mock_deployment_algorithm_client.get_keys.return_value = ["workflow1", "workflow2"]
        mock_get_deployment_manager_client.return_value = mock_deployment_algorithm_client

        client = Client()

        # Capture the output of the print statements
        with patch("builtins.print") as mocked_print:
            client.list_workflows()

        # Check that the print statements were executed with the correct arguments
        calls = [call("Deployed workflows:"), call("workflow1"), call("workflow2")]
        mocked_print.assert_has_calls(calls)

    @patch.object(Endpoints, "get_deployment_algorithm_workflow_placement_decision_client")
    @patch.object(Endpoints, "get_deployment_resources_client")
    @patch.object(Endpoints, "get_deployment_manager_client")
    @patch.object(RemoteClientFactory, "get_remote_client")
    def test_remove(
        self,
        mock_get_remote_client,
        mock_get_deployment_resources_client,
        mock_get_deployment_manager_client,
        mock_get_deployment_algorithm_workflow_placement_decision_client,
    ):
        # Mocking the scenario where the workflow id is provided and the workflow is removed successfully
        mock_deployment_algorithm_client = MagicMock()
        mock_deployment_manager_client = MagicMock()
        mock_remote_client = MagicMock()
        mock_get_deployment_algorithm_workflow_placement_decision_client.return_value = mock_deployment_algorithm_client
        mock_get_deployment_resources_client.return_value = mock_deployment_algorithm_client
        mock_get_deployment_manager_client.return_value = mock_deployment_manager_client
        mock_get_remote_client.return_value = mock_remote_client

        client = Client()
        client._workflow_id = "workflow_id"

        # Mock the return value of get_value_from_table
        mock_deployment_manager_client.get_value_from_table.return_value = (
            json.dumps(
                {
                    "deployed_regions": json.dumps(
                        {"function_instance": {"deploy_region": {"provider": "provider", "region": "region"}}}
                    )
                }
            ),
            0.0,
        )

        # Capture the output of the print statements
        with patch("builtins.print") as mocked_print:
            client.remove()

        # Check that the print statement was executed
        mocked_print.assert_called_with("Removed workflow workflow_id")

    @patch.object(RemoteClientFactory, "get_remote_client")
    def test_remove_workflow(self, mock_get_remote_client):
        # Mocking the scenario where the workflow is removed successfully
        mock_remote_client = MagicMock()
        mock_get_remote_client.return_value = mock_remote_client

        client = Client()

        # Mock the input to _remove_workflow
        deployment_manager_config_json = json.dumps(
            {
                "deployed_regions": json.dumps(
                    {"function_instance": {"deploy_region": {"provider": "provider", "region": "region"}}}
                )
            }
        )

        # Capture the output of the print statements
        with patch("builtins.print") as mocked_print:
            client._remove_workflow(deployment_manager_config_json)

        # Check that the print statement was executed
        mocked_print.assert_called_with("Removed function function_instance from provider provider in region region")

    @patch("caribou.endpoint.client.datetime")
    def test_get_deployment_key(self, mock_datetime):
        # Arrange
        client = Client()
        workflow_placement_decision = {
            "workflow_placement": {"current_deployment": {"expiry_time": "2022-01-01 00:00:00"}}
        }
        mock_datetime.now.return_value = datetime(2022, 1, 1, 0, 1, 0)
        mock_datetime.strptime.return_value = datetime(2022, 1, 1, 0, 0, 0)

        # Act & Assert
        self.assertEqual(client._get_deployment_key(workflow_placement_decision, True), "home_deployment")
        self.assertEqual(client._get_deployment_key(workflow_placement_decision, False), "home_deployment")

        # Change the current time to before the expiry time
        mock_datetime.now.return_value = datetime(2021, 12, 31, 23, 59, 59)
        self.assertEqual(client._get_deployment_key(workflow_placement_decision, False), "current_deployment")

        # Test with no expiry time
        workflow_placement_decision["workflow_placement"]["current_deployment"]["expiry_time"] = None
        self.assertEqual(client._get_deployment_key(workflow_placement_decision, False), "home_deployment")

    @patch.object(RemoteClientFactory, "get_remote_client")
    def test_remove_function_instance_success(self, mock_get_remote_client):
        # Mocking the scenario where the function instance is removed successfully
        mock_remote_client = MagicMock()
        mock_get_remote_client.return_value = mock_remote_client

        client = Client()

        # Mock the input to _remove_function_instance
        function_instance = "function_instance"
        provider_region = {"provider": "provider", "region": "region"}

        # Capture the output of the print statements
        with patch("builtins.print") as mocked_print:
            client._remove_function_instance(function_instance, provider_region)

        # Check that the print statement was executed
        mocked_print.assert_called_with("Removed function function_instance from provider provider in region region")

    @patch.object(RemoteClientFactory, "get_remote_client")
    def test_remove_function_instance_ecr_repository_error(self, mock_get_remote_client):
        # Mocking the scenario where removing the ECR repository raises an error
        mock_remote_client = MagicMock(spec=AWSRemoteClient)
        mock_remote_client.remove_ecr_repository.side_effect = RuntimeError("ECR error")
        mock_get_remote_client.return_value = mock_remote_client

        client = Client()

        # Mock the input to _remove_function_instance
        function_instance = "function_instance"
        provider_region = {"provider": "provider", "region": "region"}

        # Capture the output of the print statements
        with patch("builtins.print") as mocked_print:
            client._remove_function_instance(function_instance, provider_region)

        # Check that the print statement was executed
        mocked_print.assert_any_call("Could not remove ecr repository function_instance: ECR error")

    @patch.object(RemoteClientFactory, "get_remote_client")
    def test_remove_function_instance_messaging_topic_error(self, mock_get_remote_client):
        # Mocking the scenario where removing the messaging topic raises an error
        mock_remote_client = MagicMock()
        mock_remote_client.get_topic_identifier.return_value = "topic_identifier"
        mock_remote_client.remove_messaging_topic.side_effect = RuntimeError("Messaging topic error")
        mock_get_remote_client.return_value = mock_remote_client

        client = Client()

        # Mock the input to _remove_function_instance
        function_instance = "function_instance"
        provider_region = {"provider": "provider", "region": "region"}

        # Capture the output of the print statements
        with patch("builtins.print") as mocked_print:
            client._remove_function_instance(function_instance, provider_region)

        # Check that the print statement was executed
        mocked_print.assert_any_call(
            "Could not remove messaging topic function_instance_messaging_topic: Messaging topic error"
        )

    @patch.object(RemoteClientFactory, "get_remote_client")
    def test_remove_function_instance_function_error(self, mock_get_remote_client):
        # Mocking the scenario where removing the function raises an error
        mock_remote_client = MagicMock()
        mock_remote_client.remove_function.side_effect = RuntimeError("Function error")
        mock_get_remote_client.return_value = mock_remote_client

        client = Client()

        # Mock the input to _remove_function_instance
        function_instance = "function_instance"
        provider_region = {"provider": "provider", "region": "region"}

        # Capture the output of the print statements
        with patch("builtins.print") as mocked_print:
            client._remove_function_instance(function_instance, provider_region)

        # Check that the print statement was executed
        mocked_print.assert_any_call("Could not remove function function_instance: Function error")

    @patch.object(RemoteClientFactory, "get_remote_client")
    def test_remove_function_instance_role_error(self, mock_get_remote_client):
        # Mocking the scenario where removing the IAM role raises an error
        mock_remote_client = MagicMock()
        mock_remote_client.remove_role.side_effect = RuntimeError("Role error")
        mock_get_remote_client.return_value = mock_remote_client

        client = Client()

        # Mock the input to _remove_function_instance
        function_instance = "function_instance"
        provider_region = {"provider": "provider", "region": "region"}

        # Capture the output of the print statements
        with patch("builtins.print") as mocked_print:
            client._remove_function_instance(function_instance, provider_region)

        # Check that the print statement was executed
        mocked_print.assert_any_call("Could not remove role function_instance-role: Role error")

    @patch.object(RemoteClientFactory, "get_remote_client")
    def test_remove_function_instance_botocore_client_error(self, mock_get_remote_client):
        # Mocking the scenario where botocore client error is raised
        mock_remote_client = MagicMock(spec=AWSRemoteClient)
        mock_remote_client.remove_ecr_repository.side_effect = botocore.exceptions.ClientError(
            error_response={"Error": {"Code": "ClientError"}}, operation_name="RemoveECRRepository"
        )
        mock_get_remote_client.return_value = mock_remote_client

        client = Client()

        # Mock the input to _remove_function_instance
        function_instance = "function_instance"
        provider_region = {"provider": "provider", "region": "region"}

        # Capture the output of the print statements
        with patch("builtins.print") as mocked_print:
            client._remove_function_instance(function_instance, provider_region)

        # Check that the print statement was executed
        mocked_print.assert_any_call(
            "Could not remove ecr repository function_instance: An error occurred (ClientError) when calling the RemoveECRRepository operation: Unknown"
        )


if __name__ == "__main__":
    unittest.main()
