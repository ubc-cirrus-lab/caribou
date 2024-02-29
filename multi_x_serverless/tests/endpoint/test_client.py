import unittest
from unittest.mock import patch, MagicMock
from multi_x_serverless.endpoint.client import Client
import json
from multi_x_serverless.common.models.endpoints import Endpoints
from unittest.mock import call
from multi_x_serverless.common.models.remote_client.remote_client_factory import RemoteClientFactory


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

        self.assertEqual(
            str(context.exception),
            "No workflow placement decision found for workflow, did you deploy the workflow and is the workflow id (workflow_name) correct?",
        )

    @patch.object(Endpoints, "get_solver_update_checker_client")
    def test_list_workflows_no_workflows_deployed(self, mock_get_solver_update_checker_client):
        # Mocking the scenario where no workflows are deployed
        mock_solver_client = MagicMock()
        mock_solver_client.get_keys.return_value = None
        mock_get_solver_update_checker_client.return_value = mock_solver_client

        client = Client()

        # Capture the output of the print statements
        with patch("builtins.print") as mocked_print:
            client.list_workflows()

        # Check that the print statement in the if block was executed
        mocked_print.assert_called_once_with("No workflows deployed")

    @patch.object(Endpoints, "get_solver_update_checker_client")
    def test_list_workflows_workflows_deployed(self, mock_get_solver_update_checker_client):
        # Mocking the scenario where workflows are deployed
        mock_solver_client = MagicMock()
        mock_solver_client.get_keys.return_value = ["workflow1", "workflow2"]
        mock_get_solver_update_checker_client.return_value = mock_solver_client

        client = Client()

        # Capture the output of the print statements
        with patch("builtins.print") as mocked_print:
            client.list_workflows()

        # Check that the print statements were executed with the correct arguments
        calls = [call("Deployed workflows:"), call("workflow1"), call("workflow2")]
        mocked_print.assert_has_calls(calls)

    @patch.object(Endpoints, "get_solver_workflow_placement_decision_client")
    @patch.object(Endpoints, "get_solver_update_checker_client")
    @patch.object(Endpoints, "get_deployment_manager_client")
    @patch.object(RemoteClientFactory, "get_remote_client")
    def test_remove(
        self,
        mock_get_remote_client,
        mock_get_deployment_manager_client,
        mock_get_solver_update_checker_client,
        mock_get_solver_workflow_placement_decision_client,
    ):
        # Mocking the scenario where the workflow id is provided and the workflow is removed successfully
        mock_solver_client = MagicMock()
        mock_deployment_manager_client = MagicMock()
        mock_remote_client = MagicMock()
        mock_get_solver_workflow_placement_decision_client.return_value = mock_solver_client
        mock_get_solver_update_checker_client.return_value = mock_solver_client
        mock_get_deployment_manager_client.return_value = mock_deployment_manager_client
        mock_get_remote_client.return_value = mock_remote_client

        client = Client()
        client._workflow_id = "workflow_id"

        # Mock the return value of get_all_values_from_table
        mock_deployment_manager_client.get_all_values_from_table.return_value = {
            "workflow_id": json.dumps(
                {
                    "deployed_regions": json.dumps(
                        {"function_instance": {"deploy_region": {"provider": "provider", "region": "region"}}}
                    )
                }
            )
        }

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

    @patch.object(RemoteClientFactory, "get_remote_client")
    def test_remove_function_instance(self, mock_get_remote_client):
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

    @patch.object(Endpoints, "get_solver_update_checker_client")
    def test_solve(self, mock_get_solver_update_checker_client):
        # Mocking the scenario where the workflow id is provided and the solver is solved successfully
        mock_solver_client = MagicMock()
        mock_get_solver_update_checker_client.return_value = mock_solver_client

        client = Client()
        client._workflow_id = "workflow_id"

        # Mock the return value of get_value_from_table
        mock_solver_client.get_value_from_table.return_value = json.dumps(
            {"workflow_config": json.dumps({"key": "value"})}
        )

        # Mock the solver classes
        with patch("multi_x_serverless.endpoint.client.CoarseGrainedSolver") as MockCoarseGrainedSolver, patch(
            "multi_x_serverless.endpoint.client.BFSFineGrainedSolver"
        ) as MockBFSFineGrainedSolver, patch(
            "multi_x_serverless.endpoint.client.StochasticHeuristicDescentSolver"
        ) as MockStochasticHeuristicDescentSolver, patch(
            "multi_x_serverless.endpoint.client.WorkflowConfig"
        ) as MockWorkflowConfig:
            mock_coarse_grained_solver = MockCoarseGrainedSolver.return_value
            mock_bfs_fine_grained_solver = MockBFSFineGrainedSolver.return_value
            mock_stochastic_heuristic_descent_solver = MockStochasticHeuristicDescentSolver.return_value

            # Test with solver=None
            client.solve()
            mock_coarse_grained_solver.solve.assert_called_once()

            # Test with solver="coarse-grained"
            client.solve("coarse-grained")
            mock_coarse_grained_solver.solve.assert_called()

            # Test with solver="fine-grained"
            client.solve("fine-grained")
            mock_bfs_fine_grained_solver.solve.assert_called_once()

            # Test with solver="heuristic"
            client.solve("heuristic")
            mock_stochastic_heuristic_descent_solver.solve.assert_called_once()

            # Test with unsupported solver
            with self.assertRaises(ValueError) as context:
                client.solve("unsupported_solver")
            self.assertEqual(str(context.exception), "Solver unsupported_solver not supported")


if __name__ == "__main__":
    unittest.main()
