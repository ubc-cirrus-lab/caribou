import unittest
from unittest.mock import MagicMock, patch, PropertyMock
from multi_x_serverless.routing.deployment_algorithms.stochastic_heuristic_deployment_algorithm import (
    StochasticHeuristicDeploymentAlgorithm,
)
from multi_x_serverless.routing.deployment_algorithms.deployment_algorithm import DeploymentAlgorithm
from multi_x_serverless.routing.workflow_config import WorkflowConfig


class TestStochasticHeuristicDeploymentAlgorithm(unittest.TestCase):
    @patch.object(StochasticHeuristicDeploymentAlgorithm, "__init__", return_value=None)
    def setUp(self, mock_super_init):
        mock_workflow_config = MagicMock(spec=WorkflowConfig)
        self._algorithm = StochasticHeuristicDeploymentAlgorithm(mock_workflow_config)
        self._algorithm._region_indexer = MagicMock()
        self._algorithm._instance_indexer = MagicMock()

    def test_setup(self):
        # Arrange
        self._algorithm._number_of_instances = 3
        self._algorithm._region_indexer.get_value_indices.return_value = {1: 1, 2: 2}
        self._algorithm._instance_indexer.get_value_indices.return_value = {1: 1, 2: 2}
        self._algorithm._temperature = 1.0

        # Act
        self._algorithm._setup()

        # Assert
        self.assertEqual(self._algorithm._learning_rate, 1)
        self.assertEqual(self._algorithm._num_iterations, 16)
        self.assertEqual(self._algorithm._temperature, 1.0)

    def test_run_algorithm(self):
        # Arrange
        self._algorithm._generate_stochastic_heuristic_deployments = MagicMock()
        self._algorithm._generate_stochastic_heuristic_deployments.return_value = [
            ([1, 1, 1], {"metric1": 1.0, "metric2": 2.0})
        ]
        self._algorithm._number_of_instances = 3

        # Act
        result = self._algorithm._run_algorithm()

        # Assert
        expected_result = [([1, 1, 1], {"metric1": 1.0, "metric2": 2.0})]
        self.assertEqual(result, expected_result)

    def test_generate_stochastic_heuristic_deployments(self):
        # Arrange
        self._algorithm._home_deployment_metrics = {"metric1": 1.0, "metric2": 2.0}
        self._algorithm._home_deployment = [1, 1, 1]
        self._algorithm._num_iterations = 2
        self._algorithm._generate_new_deployment = MagicMock()
        self._algorithm._generate_new_deployment.return_value = [2, 2, 2]
        self._algorithm._deployment_metrics_calculator = MagicMock()
        self._algorithm._deployment_metrics_calculator.calculate_deployment_metrics.return_value = {
            "metric1": 2.0,
            "metric2": 3.0,
        }
        self._algorithm._is_hard_constraint_failed = MagicMock()
        self._algorithm._is_hard_constraint_failed.return_value = False
        self._algorithm._is_improvement = MagicMock()
        self._algorithm._is_improvement.return_value = True
        self._algorithm._number_of_instances = 3
        self._algorithm._temperature = 0.99

        # Act
        result = self._algorithm._generate_stochastic_heuristic_deployments()

        # Assert
        expected_result = [([2, 2, 2], {"metric1": 2.0, "metric2": 3.0})]
        self.assertEqual(result, expected_result)

    def test_is_improvement(self):
        # Arrange
        self._algorithm._ranker = MagicMock()
        self._algorithm._ranker.number_one_priority = "metric1"
        current_deployment_metrics = {"metric1": 1.0, "metric2": 2.0}
        new_deployment_metrics = {"metric1": 0.5, "metric2": 1.5}

        # Act
        result = self._algorithm._is_improvement(current_deployment_metrics, new_deployment_metrics)

        # Assert
        self.assertTrue(result)

    def test_acceptance_probability(self):
        # Arrange
        self._algorithm._temperature = 0

        # Act
        result = self._algorithm._acceptance_probability()

        # Assert
        self.assertEqual(result, 1.0)

    def test_generate_new_deployment(self):
        # Arrange
        self._algorithm._learning_rate = 1
        self._algorithm._choose_new_region = MagicMock()
        self._algorithm._choose_new_region.return_value = 2
        self._algorithm._number_of_instances = 3
        current_deployment = [1, 1, 1]

        # Act
        result = self._algorithm._generate_new_deployment(current_deployment)

        # Assert
        self.assertEqual(result.count(2), 1)

    def test_choose_new_region(self):
        # Arrange
        self._algorithm._per_instance_permitted_regions = [[0, 1, 2], [0, 1, 2], [0, 1, 2]]

        # Act
        result = self._algorithm._choose_new_region(1)

        # Assert
        self.assertIn(result, [0, 1, 2])


if __name__ == "__main__":
    unittest.main()