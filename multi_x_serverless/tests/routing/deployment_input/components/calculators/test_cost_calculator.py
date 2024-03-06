import unittest
from unittest.mock import MagicMock
import numpy as np
from multi_x_serverless.routing.deployment_input.components.calculators.cost_calculator import CostCalculator
from multi_x_serverless.routing.deployment_input.components.loaders.datacenter_loader import DatacenterLoader
from multi_x_serverless.routing.deployment_input.components.loaders.workflow_loader import WorkflowLoader
from multi_x_serverless.routing.deployment_input.components.calculators.runtime_calculator import RuntimeCalculator


class TestCostCalculator(unittest.TestCase):
    def test_calculate_execution_cost_distribution(self):
        # Arrange
        mock_datacenter_loader = MagicMock(spec=DatacenterLoader)
        mock_workflow_loader = MagicMock(spec=WorkflowLoader)
        mock_runtime_calculator = MagicMock(spec=RuntimeCalculator)
        cost_calculator = CostCalculator(mock_datacenter_loader, mock_workflow_loader, mock_runtime_calculator)
        mock_runtime_calculator.calculate_runtime_distribution.return_value = np.array([0.2, 0.1, 0.3])
        mock_workflow_loader.get_vcpu.return_value = 2.0
        mock_workflow_loader.get_memory.return_value = 2048.0
        mock_workflow_loader.get_architecture.return_value = "x86"
        mock_datacenter_loader.get_compute_cost.return_value = 0.01
        mock_datacenter_loader.get_invocation_cost.return_value = 0.00001

        # Act
        execution_cost_distribution = cost_calculator.calculate_execution_cost_distribution(
            "instance1", "AWS:us-east-1"
        )

        # Assert
        np.testing.assert_array_almost_equal(execution_cost_distribution, np.array([0.00401, 0.00801, 0.01201]))

    def test_calculate_transmission_cost_distribution(self):
        # Arrange
        mock_datacenter_loader = MagicMock(spec=DatacenterLoader)
        mock_workflow_loader = MagicMock(spec=WorkflowLoader)
        mock_runtime_calculator = MagicMock(spec=RuntimeCalculator)
        cost_calculator = CostCalculator(mock_datacenter_loader, mock_workflow_loader, mock_runtime_calculator)
        mock_workflow_loader.get_data_transfer_size_distribution.return_value = [0.2, 0.1, 0.3]
        mock_datacenter_loader.get_transmission_cost.return_value = 0.01

        # Act
        transmission_cost_distribution = cost_calculator.calculate_transmission_cost_distribution(
            "instance1", "instance2", "AWS:us-east-1", "AWS:us-west-2"
        )

        # Assert
        np.testing.assert_array_almost_equal(transmission_cost_distribution, np.array([0.001, 0.002, 0.003]))


if __name__ == "__main__":
    unittest.main()
