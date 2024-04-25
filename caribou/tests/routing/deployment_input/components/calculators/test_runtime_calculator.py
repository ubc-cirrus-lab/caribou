import unittest
from unittest.mock import MagicMock, patch
import numpy as np
from caribou.routing.deployment_input.components.calculators.runtime_calculator import RuntimeCalculator
from caribou.routing.deployment_input.components.loaders.performance_loader import PerformanceLoader
from caribou.routing.deployment_input.components.loaders.workflow_loader import WorkflowLoader


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
        np.testing.assert_array_equal(runtime_distribution, np.array([0.2, 0.1, 0.3]))

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

    def test_get_transmission_size_distribution(self):
        # Arrange
        mock_performance_loader = MagicMock(spec=PerformanceLoader)
        mock_workflow_loader = MagicMock(spec=WorkflowLoader)
        runtime_calculator = RuntimeCalculator(mock_performance_loader, mock_workflow_loader)
        mock_workflow_loader.get_data_transfer_size_distribution.return_value = [0.2, 0.1, 0.3]

        # Act
        transmission_size_distribution = runtime_calculator.get_transmission_size_distribution(
            "instance1", "instance2", "region1", "region2"
        )

        # Assert
        np.testing.assert_array_equal(transmission_size_distribution, np.array([0.2, 0.1, 0.3]))
        mock_workflow_loader.get_data_transfer_size_distribution.assert_called_once_with(
            "instance1", "instance2", "region1", "region2"
        )

    def test_get_transmission_size_distribution_no_from_instance(self):
        # Arrange
        mock_performance_loader = MagicMock(spec=PerformanceLoader)
        mock_workflow_loader = MagicMock(spec=WorkflowLoader)
        runtime_calculator = RuntimeCalculator(mock_performance_loader, mock_workflow_loader)
        mock_workflow_loader.get_start_hop_size_distribution.return_value = [0.2, 0.1, 0.3]

        # Act
        transmission_size_distribution = runtime_calculator.get_transmission_size_distribution(
            None, "instance2", "region1", "region2"
        )

        # Assert
        np.testing.assert_array_equal(transmission_size_distribution, np.array([0.2, 0.1, 0.3]))
        mock_workflow_loader.get_start_hop_size_distribution.assert_called_once_with("region2")

    def test_get_transmission_latency_distribution(self):
        # Arrange
        mock_performance_loader = MagicMock(spec=PerformanceLoader)
        mock_workflow_loader = MagicMock(spec=WorkflowLoader)
        runtime_calculator = RuntimeCalculator(mock_performance_loader, mock_workflow_loader)
        mock_workflow_loader.get_latency_distribution.return_value = [0.2, 0.1, 0.3]

        # Act
        transmission_latency_distribution = runtime_calculator.get_transmission_latency_distribution(
            "instance1", "instance2", "region1", "region2", 1.0
        )

        # Assert
        np.testing.assert_array_equal(transmission_latency_distribution, np.array([0.2, 0.1, 0.3]))
        mock_workflow_loader.get_latency_distribution.assert_called_once_with(
            "instance1", "instance2", "region1", "region2", 1.0
        )

    def test_get_transmission_latency_distribution_no_from_instance(self):
        # Arrange
        mock_performance_loader = MagicMock(spec=PerformanceLoader)
        mock_workflow_loader = MagicMock(spec=WorkflowLoader)
        runtime_calculator = RuntimeCalculator(mock_performance_loader, mock_workflow_loader)
        mock_workflow_loader.get_start_hop_latency_distribution.return_value = [0.2, 0.1, 0.3]

        # Act
        transmission_latency_distribution = runtime_calculator.get_transmission_latency_distribution(
            None, "instance2", "region1", "region2", 1.0
        )

        # Assert
        np.testing.assert_array_equal(transmission_latency_distribution, np.array([0.2, 0.1, 0.3]))
        mock_workflow_loader.get_start_hop_latency_distribution.assert_called_once_with("region2", 1.0)

    @patch("random.random", return_value=0.2)
    def test_get_transmission_latency_distribution_with_data_transfer_size(self, mock_random):
        # Arrange
        mock_performance_loader = MagicMock(spec=PerformanceLoader)
        mock_workflow_loader = MagicMock(spec=WorkflowLoader)
        runtime_calculator = RuntimeCalculator(mock_performance_loader, mock_workflow_loader)
        mock_performance_loader.get_transmission_latency_distribution.return_value = [0.2, 0.1, 0.3]
        mock_workflow_loader.get_home_region.return_value = "region1"
        mock_workflow_loader.get_data_transfer_size_distribution.return_value = [100, 200, 300]
        mock_workflow_loader.get_latency_distribution.return_value = [0.1, 0.2, 0.3]
        mock_random.return_value = 0  # Always select the first element

        # Act
        transmission_latency_distribution = runtime_calculator.get_transmission_latency_distribution(
            "instance1", "instance2", "region1", "region2", None
        )

        # Assert
        np.testing.assert_array_almost_equal(transmission_latency_distribution, np.array([0.2, 0.1, 0.3]))
        mock_performance_loader.get_transmission_latency_distribution.assert_called_with("region1", "region1")
        mock_workflow_loader.get_home_region.assert_called_once()
        mock_workflow_loader.get_data_transfer_size_distribution.assert_called_once_with(
            "instance1", "instance2", "region1", "region1"
        )
        mock_workflow_loader.get_latency_distribution.assert_called_once_with(
            "instance1", "instance2", "region1", "region1", 100
        )


if __name__ == "__main__":
    unittest.main()