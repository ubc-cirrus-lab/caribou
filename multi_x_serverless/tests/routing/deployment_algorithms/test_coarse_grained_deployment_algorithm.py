import unittest
from unittest.mock import MagicMock, patch
from multi_x_serverless.routing.deployment_algorithms.coarse_grained_deployment_algorithm import (
    CoarseGrainedDeploymentAlgorithm,
)


class TestCoarseGrainedDeploymentAlgorithm(unittest.TestCase):
    @patch("multi_x_serverless.routing.deployment_input.input_manager.Endpoints")
    @patch("multiprocessing.Pool")
    def test_run_algorithm(self, mock_pool, mock_endpoints):
        mock_workflow_config = MagicMock()

        # Create a mock for the _generate_and_check_deployment method
        mock_generate_and_check_deployment = MagicMock()
        mock_generate_and_check_deployment.return_value = ([1, 2, 3], {"metric1": 1.0, "metric2": 2.0})

        # Create a mock for the _region_indexer attribute
        mock_region_indexer = MagicMock()
        mock_region_indexer.get_value_indices.return_value = {1: 1, 2: 2, 3: 3}

        # Create a mock for the _per_instance_permitted_regions attribute
        mock_per_instance_permitted_regions = {0: [1, 2, 3], 1: [1, 2, 3]}

        # Create a mock for the _deployment_metrics_calculator attribute
        mock_deployment_metrics_calculator = MagicMock()
        mock_deployment_metrics_calculator.calculate_deployment_metrics.return_value = {"metric1": 1.0, "metric2": 2.0}

        # Create a mock for the _is_hard_constraint_failed method
        mock_is_hard_constraint_failed = MagicMock()
        mock_is_hard_constraint_failed.return_value = False

        mock_endpoints.return_value = MagicMock()

        # Create an instance of CoarseGrainedDeploymentAlgorithm
        algorithm = CoarseGrainedDeploymentAlgorithm(mock_workflow_config)

        # Set the mocks on the instance
        algorithm._generate_and_check_deployment = mock_generate_and_check_deployment
        algorithm._region_indexer = mock_region_indexer
        algorithm._per_instance_permitted_regions = mock_per_instance_permitted_regions
        algorithm._deployment_metrics_calculator = mock_deployment_metrics_calculator
        algorithm._is_hard_constraint_failed = mock_is_hard_constraint_failed

        # Call the _run_algorithm method
        result = algorithm._run_algorithm()

        # Check the result
        expected_result = [([1, 2, 3], {"metric1": 1.0, "metric2": 2.0})]
        self.assertEqual(result, expected_result)


if __name__ == "__main__":
    unittest.main()
