import unittest
from unittest.mock import MagicMock
import numpy as np
from multi_x_serverless.routing.deployment_input.components.calculators.runtime_calculator import RuntimeCalculator
from multi_x_serverless.routing.deployment_input.components.loaders.performance_loader import PerformanceLoader
from multi_x_serverless.routing.deployment_input.components.loaders.workflow_loader import WorkflowLoader


class TestRuntimeCalculator(unittest.TestCase):
    def test_calculate_runtime_distribution(self):
        # Arrange
        mock_performance_loader = MagicMock(spec=PerformanceLoader)
        mock_workflow_loader = MagicMock(spec=WorkflowLoader)
        runtime_calculator = RuntimeCalculator(mock_performance_loader, mock_workflow_loader)
        mock_workflow_loader.get_runtime_distribution.side_effect = [[], [0.2, 0.1, 0.3]]

        # Act
        runtime_distribution = runtime_calculator.calculate_runtime_distribution("instance1", "region1")

        # Assert
        np.testing.assert_array_equal(runtime_distribution, np.array([0.1, 0.2, 0.3]))

    def test_calculate_runtime_distribution_no_runtime_data(self):
        # Arrange
        mock_performance_loader = MagicMock(spec=PerformanceLoader)
        mock_workflow_loader = MagicMock(spec=WorkflowLoader)
        runtime_calculator = RuntimeCalculator(mock_performance_loader, mock_workflow_loader)
        mock_workflow_loader.get_runtime_distribution.side_effect = [[], []]

        # Act
        runtime_distribution = runtime_calculator.calculate_runtime_distribution("instance1", "region1")

        # Assert
        np.testing.assert_array_equal(runtime_distribution, np.array([0.0]))


if __name__ == "__main__":
    unittest.main()
