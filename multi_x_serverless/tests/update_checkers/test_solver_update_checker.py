from unittest.mock import patch, MagicMock
import json
import unittest
from multi_x_serverless.update_checkers.solver_update_checker import SolverUpdateChecker
from multi_x_serverless.endpoint.client import Endpoints


class TestSolverUpdateChecker(unittest.TestCase):
    @patch.object(Endpoints, "get_solver_update_checker_client")
    @patch.object(Endpoints, "get_data_collector_client")
    def test_check(self, mock_get_data_collector_client, mock_get_solver_update_checker_client):
        # Mocking the scenario where the workflow ids are retrieved and the solver is solved successfully
        mock_solver_client = MagicMock()
        mock_data_collector_client = MagicMock()
        mock_get_solver_update_checker_client.return_value = mock_solver_client
        mock_get_data_collector_client.return_value = mock_data_collector_client

        checker = SolverUpdateChecker()

        # Mock the return values of get_keys and get_value_from_table
        mock_solver_client.get_keys.return_value = ["workflow_id"]
        mock_data_collector_client.get_value_from_table.side_effect = [
            json.dumps({"solver": "coarse_grained_solver", "num_calls_in_one_month": 10}),  # workflow_config
            json.dumps({"months_between_summary": 1, "total_invocations": 20}),  # workflow_summary
        ]

        # Mock the solver classes
        with patch(
            "multi_x_serverless.update_checkers.solver_update_checker.CoarseGrainedSolver"
        ) as MockCoarseGrainedSolver, patch("multi_x_serverless.endpoint.client.WorkflowConfig"), patch(
            "multi_x_serverless.update_checkers.solver_update_checker.WorkflowConfig"
        ) as MockWorkflowConfig:
            mock_coarse_grained_solver = MockCoarseGrainedSolver.return_value
            workflow_config = MockWorkflowConfig.return_value
            workflow_config.num_calls_in_one_month = 10
            workflow_config.solver = "coarse_grained_solver"

            checker.check()

            # Check that the solve method was called on the solver instance
            mock_coarse_grained_solver.solve.assert_called_once()


if __name__ == "__main__":
    unittest.main()
