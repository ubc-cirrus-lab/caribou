import unittest
from unittest.mock import MagicMock, patch
from caribou.routing.deployment_algorithms.coarse_grained_deployment_algorithm import (
    CoarseGrainedDeploymentAlgorithm,
)


class TestCoarseGrainedDeploymentAlgorithm(unittest.TestCase):
    @patch(
        "caribou.routing.deployment_algorithms.coarse_grained_deployment_algorithm.CoarseGrainedDeploymentAlgorithm.__init__",
        return_value=None,
    )
    def setUp(self, mock_init):
        mock_workflow_config = MagicMock()
        self._algorithm = CoarseGrainedDeploymentAlgorithm(mock_workflow_config)

    @patch("multiprocessing.Pool")
    def test_run_algorithm(self, mock_pool):
        # Create a mock for the _generate_and_check_deployment method
        mock_generate_and_check_deployment = MagicMock()
        mock_generate_and_check_deployment.return_value = ([1, 1, 1], {"metric1": 1.0, "metric2": 2.0})

        # Create a mock for the _region_indexer attribute
        mock_region_indexer = MagicMock()
        mock_region_indexer.get_value_indices.return_value = {1: 1}

        # Set the mocks on the instance
        self._algorithm._generate_and_check_deployment = mock_generate_and_check_deployment
        self._algorithm._region_indexer = mock_region_indexer

        # Call the _run_algorithm method
        result = self._algorithm._run_algorithm()

        # Check the result
        expected_result = [([1, 1, 1], {"metric1": 1.0, "metric2": 2.0})]
        self.assertEqual(result, expected_result)

    def test_generate_and_check_deployment(self):
        # Arrange
        self._algorithm._per_instance_permitted_regions = [[0, 1, 2], [0, 1, 2]]
        self._algorithm._number_of_instances = 2
        self._algorithm._generate_deployment = MagicMock()
        self._algorithm._generate_deployment.return_value = [1, 1]
        self._algorithm._deployment_metrics_calculator = MagicMock()
        self._algorithm._deployment_metrics_calculator.calculate_deployment_metrics.return_value = {
            "metric1": 1.0,
            "metric2": 2.0,
        }
        self._algorithm._is_hard_constraint_failed = MagicMock()
        self._algorithm._is_hard_constraint_failed.return_value = False

        # Act
        result = self._algorithm._generate_and_check_deployment(1)

        # Assert
        expected_result = ([1, 1], {"metric1": 1.0, "metric2": 2.0})
        self.assertEqual(result, expected_result)

    def test_generate_and_check_deployment_positive(self):
        # Arrange
        self._algorithm._per_instance_permitted_regions = [[0, 1, 2], [0, 1, 2]]
        self._algorithm._number_of_instances = 2
        self._algorithm._generate_deployment = MagicMock()
        self._algorithm._generate_deployment.return_value = [1, 1]
        self._algorithm._deployment_metrics_calculator = MagicMock()
        self._algorithm._deployment_metrics_calculator.calculate_deployment_metrics.return_value = {
            "metric1": 1.0,
            "metric2": 2.0,
        }
        self._algorithm._is_hard_constraint_failed = MagicMock()
        self._algorithm._is_hard_constraint_failed.return_value = False

        # Act
        result = self._algorithm._generate_and_check_deployment(1)

        # Assert
        expected_result = ([1, 1], {"metric1": 1.0, "metric2": 2.0})
        self.assertEqual(result, expected_result)

    def test_generate_and_check_deployment_negative(self):
        # Arrange
        self._algorithm._per_instance_permitted_regions = [[0, 2], [0, 1, 2]]
        self._algorithm._number_of_instances = 2
        self._algorithm._generate_deployment = MagicMock()
        self._algorithm._generate_deployment.return_value = [1, 1]
        self._algorithm._deployment_metrics_calculator = MagicMock()
        self._algorithm._deployment_metrics_calculator.calculate_deployment_metrics.return_value = {
            "metric1": 1.0,
            "metric2": 2.0,
        }
        self._algorithm._is_hard_constraint_failed = MagicMock()
        self._algorithm._is_hard_constraint_failed.return_value = False

        # Act
        result = self._algorithm._generate_and_check_deployment(1)

        # Assert
        self.assertIsNone(result)

    def test_generate_deployment(self):
        # Arrange
        self._algorithm._number_of_instances = 3

        # Act
        result = self._algorithm._generate_deployment(1)

        # Assert
        expected_result = [1, 1, 1]
        self.assertEqual(result, expected_result)


if __name__ == "__main__":
    unittest.main()
