import unittest
from unittest.mock import MagicMock, patch
from multi_x_serverless.routing.deployment_input.components.calculators.cost_calculator import CostCalculator
from multi_x_serverless.routing.deployment_input.components.loaders.datacenter_loader import DatacenterLoader
from multi_x_serverless.routing.deployment_input.components.loaders.workflow_loader import WorkflowLoader
from multi_x_serverless.routing.deployment_input.components.calculators.runtime_calculator import RuntimeCalculator


class TestCostCalculator(unittest.TestCase):
    def setUp(self):
        # Create the CostCalculator object
        self.datacenter_loader = MagicMock(spec=DatacenterLoader)
        self.workflow_loader = MagicMock(spec=WorkflowLoader)
        self.runtime_calculator = MagicMock(spec=RuntimeCalculator)
        self.cost_calculator = CostCalculator(self.datacenter_loader, self.workflow_loader, self.runtime_calculator)

    def test_init(self):
        # Check that the attributes were initialized correctly
        self.assertEqual(self.cost_calculator._datacenter_loader, self.datacenter_loader)
        self.assertEqual(self.cost_calculator._workflow_loader, self.workflow_loader)
        self.assertEqual(self.cost_calculator._runtime_calculator, self.runtime_calculator)
        self.assertEqual(self.cost_calculator._execution_conversion_ratio_cache, {})
        self.assertEqual(self.cost_calculator._transmission_conversion_ratio_cache, {})

    @patch.object(CostCalculator, "_get_transmission_conversion_ratio")
    def test_calculate_transmission_cost(self, mock_get_transmission_conversion_ratio):
        # Set up the mock
        mock_get_transmission_conversion_ratio.return_value = 1.0

        # Call the method
        result = self.cost_calculator.calculate_transmission_cost("from_region_name", "to_region_name", 2.0)

        # Check that the result is correct
        self.assertEqual(result, 1.0 * 2.0)

        # Check that the mock was called with the correct arguments
        mock_get_transmission_conversion_ratio.assert_called_once_with("from_region_name", "to_region_name")

    @patch.object(CostCalculator, "_get_execution_conversion_ratio")
    def test_calculate_execution_cost(self, mock_get_execution_conversion_ratio):
        # Set up the mock
        mock_get_execution_conversion_ratio.return_value = (1.0, 2.0)

        # Call the method
        result = self.cost_calculator.calculate_execution_cost("instance_name", "region_name", 3.0)

        # Check that the result is correct
        self.assertEqual(result, 1.0 * 3.0 + 2.0)

        # Check that the mock was called with the correct arguments
        mock_get_execution_conversion_ratio.assert_called_once_with("instance_name", "region_name")

    def test_get_transmission_conversion_ratio(self):
        # Set up the mock
        self.cost_calculator._datacenter_loader.get_transmission_cost.return_value = 1.0

        # Call the method
        result = self.cost_calculator._get_transmission_conversion_ratio(
            "provider1:from_region_name", "provider2:to_region_name"
        )

        # Check that the result is correct
        self.assertEqual(result, 1.0)

        # Check that the mock was called with the correct arguments
        self.cost_calculator._datacenter_loader.get_transmission_cost.assert_called_once_with(
            "provider2:to_region_name", False
        )

        # Check that the _transmission_conversion_ratio_cache attribute was updated correctly
        self.assertEqual(
            self.cost_calculator._transmission_conversion_ratio_cache,
            {"provider1:from_region_name_provider2:to_region_name": 1.0},
        )

    def test_get_execution_conversion_ratio(self):
        # Set up the mocks
        self.cost_calculator._workflow_loader.get_memory.return_value = 2048.0
        self.cost_calculator._workflow_loader.get_architecture.return_value = "architecture"
        self.cost_calculator._datacenter_loader.get_compute_cost.return_value = 3.0
        self.cost_calculator._datacenter_loader.get_invocation_cost.return_value = 4.0

        # Call the method
        result = self.cost_calculator._get_execution_conversion_ratio("instance_name", "provider:region_name")

        # Check that the result is correct
        self.assertEqual(result, (3.0 * 2.0, 4.0))

        # Check that the mocks were called with the correct arguments
        self.cost_calculator._workflow_loader.get_memory.assert_called_once_with("instance_name", "provider")
        self.cost_calculator._workflow_loader.get_architecture.assert_called_once_with("instance_name", "provider")
        self.cost_calculator._datacenter_loader.get_compute_cost.assert_called_once_with(
            "provider:region_name", "architecture"
        )
        self.cost_calculator._datacenter_loader.get_invocation_cost.assert_called_once_with(
            "provider:region_name", "architecture"
        )

        # Check that the _execution_conversion_ratio_cache attribute was updated correctly
        self.assertEqual(
            self.cost_calculator._execution_conversion_ratio_cache,
            {"instance_name_provider:region_name": (3.0 * 2.0, 4.0)},
        )


if __name__ == "__main__":
    unittest.main()
