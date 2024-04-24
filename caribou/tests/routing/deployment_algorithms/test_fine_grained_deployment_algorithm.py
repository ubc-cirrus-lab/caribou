import unittest
from unittest.mock import MagicMock, patch
from caribou.routing.deployment_algorithms.fine_grained_deployment_algorithm import (
    FineGrainedDeploymentAlgorithm,
)


class TestFineGrainedDeploymentAlgorithm(unittest.TestCase):
    @patch(
        "caribou.routing.deployment_algorithms.fine_grained_deployment_algorithm.FineGrainedDeploymentAlgorithm.__init__",
        return_value=None,
    )
    def setUp(self, mock_init):
        mock_workflow_config = MagicMock()
        self._algorithm = FineGrainedDeploymentAlgorithm(mock_workflow_config)

    def test_run_algorithm(self):
        # Arrange
        self._algorithm._generate_all_possible_fine_deployments = MagicMock()
        self._algorithm._generate_all_possible_fine_deployments.return_value = [
            ([1, 1, 1], {"metric1": 1.0, "metric2": 2.0})
        ]

        # Act
        result = self._algorithm._run_algorithm()

        # Assert
        expected_result = [([1, 1, 1], {"metric1": 1.0, "metric2": 2.0})]
        self.assertEqual(result, expected_result)

    def test_generate_all_possible_fine_deployments(self):
        # Arrange
        self._algorithm._region_indexer = MagicMock()
        self._algorithm._region_indexer.get_value_indices.return_value = {1: 1, 2: 2}
        self._algorithm._number_of_instances = 2
        self._algorithm._generate_and_check_deployment = MagicMock()
        self._algorithm._generate_and_check_deployment.side_effect = [
            ([1, 1], {"metric1": 1.0, "metric2": 2.0}),
            None,
            ([2, 1], {"metric1": 1.0, "metric2": 2.0}),
            None,
        ]

        # Act
        result = self._algorithm._generate_all_possible_fine_deployments()

        # Assert
        expected_result = [([1, 1], {"metric1": 1.0, "metric2": 2.0}), ([2, 1], {"metric1": 1.0, "metric2": 2.0})]
        self.assertEqual(result, expected_result)

    def test_generate_and_check_deployment_positive(self):
        # Arrange
        self._algorithm._per_instance_permitted_regions = [[0, 1, 2], [0, 1, 2]]
        self._algorithm._number_of_instances = 2
        self._algorithm._deployment_metrics_calculator = MagicMock()
        self._algorithm._deployment_metrics_calculator.calculate_deployment_metrics.return_value = {
            "metric1": 1.0,
            "metric2": 2.0,
        }
        self._algorithm._is_hard_constraint_failed = MagicMock()
        self._algorithm._is_hard_constraint_failed.return_value = False

        # Act
        result = self._algorithm._generate_and_check_deployment((1, 1))

        # Assert
        expected_result = ([1, 1], {"metric1": 1.0, "metric2": 2.0})
        self.assertEqual(result, expected_result)

    def test_generate_and_check_deployment_negative(self):
        # Arrange
        self._algorithm._per_instance_permitted_regions = [[0, 2], [0, 1, 2]]
        self._algorithm._number_of_instances = 2
        self._algorithm._deployment_metrics_calculator = MagicMock()
        self._algorithm._deployment_metrics_calculator.calculate_deployment_metrics.return_value = {
            "metric1": 1.0,
            "metric2": 2.0,
        }
        self._algorithm._is_hard_constraint_failed = MagicMock()
        self._algorithm._is_hard_constraint_failed.return_value = False

        # Act
        result = self._algorithm._generate_and_check_deployment((1, 1))

        # Assert
        self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()
