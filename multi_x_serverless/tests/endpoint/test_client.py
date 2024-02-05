import unittest
from unittest.mock import patch, MagicMock
from multi_x_serverless.endpoint.client import Client
import json
from multi_x_serverless.common.models.endpoints import Endpoints


class TestClient(unittest.TestCase):
    @patch("multi_x_serverless.deployment.client.multi_x_serverless_workflow.RemoteClientFactory.get_remote_client")
    @patch.object(Endpoints, "get_solver_workflow_placement_decision_client")
    def test_successful_workflow_placement_decision_retrieval_and_invocation(
        self, mock_get_solver_workflow_placement_decision_client, mock_get_remote_client
    ):
        mock_solver_client = MagicMock()
        mock_solver_client.get_value_from_table.return_value = json.dumps(
            {
                "current_instance_name": "instance1",
                "workflow_placement": {
                    "instance1": {
                        "provider_region": {"provider": "aws", "region": "us-east-1"},
                        "identifier": "function1",
                    }
                },
            }
        )
        mock_get_solver_workflow_placement_decision_client.return_value = mock_solver_client

        # Mocking the remote client invocation
        mock_remote_client = MagicMock()
        mock_get_remote_client.return_value = mock_remote_client

        client = Client("workflow_name")
        client.run({"key": "value"})

        # Verify the remote client was invoked with the correct parameters
        mock_get_remote_client.assert_called_with("aws", "us-east-1")
        mock_remote_client.invoke_function.assert_called_once_with(
            message=json.dumps({"key": "value"}),
            identifier="function1",
            workflow_instance_id="0",
        )

    @patch.object(Endpoints, "get_solver_workflow_placement_decision_client")
    def test_no_workflow_placement_decision_found(self, mock_get_solver_workflow_placement_decision_client):
        # Mocking the scenario where no workflow placement decision is found
        mock_solver_client = MagicMock()
        mock_solver_client.get_value_from_table.return_value = None
        mock_get_solver_workflow_placement_decision_client.return_value = mock_solver_client

        client = Client("workflow_name")

        with self.assertRaises(RuntimeError) as context:
            client.run({"key": "value"})

        self.assertEqual(str(context.exception), "No workflow placement decision found for workflow")


if __name__ == "__main__":
    unittest.main()
