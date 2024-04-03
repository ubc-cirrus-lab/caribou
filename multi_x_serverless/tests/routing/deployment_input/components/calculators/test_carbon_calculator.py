import unittest
from unittest.mock import Mock, patch

from multi_x_serverless.routing.deployment_input.components.calculators.carbon_calculator import CarbonCalculator


class TestCarbonCalculator(unittest.TestCase):
    def setUp(self):
        self.carbon_loader = Mock()
        self.datacenter_loader = Mock()
        self.workflow_loader = Mock()
        self.runtime_calculator = Mock()
        self.carbon_calculator = CarbonCalculator(
            self.carbon_loader, self.datacenter_loader, self.workflow_loader, self.runtime_calculator, True
        )

    def test_alter_carbon_setting(self):
        # Call the method
        self.carbon_calculator.alter_carbon_setting("new_setting")

        # Check that the _hourly_carbon_setting attribute was updated correctly
        self.assertEqual(self.carbon_calculator._hourly_carbon_setting, "new_setting")

    @patch.object(CarbonCalculator, "_get_execution_conversion_ratio")
    def test_calculate_execution_carbon(self, mock_get_execution_conversion_ratio):
        # Set up the mock
        mock_get_execution_conversion_ratio.return_value = (1.0, 2.0, 3.0)

        # Call the method
        result = self.carbon_calculator.calculate_execution_carbon("instance_name", "region_name", 4.0)

        # Check that the result is correct
        self.assertEqual(result, 4.0 * (1.0 + 2.0) * 3.0)

        # Check that the mock was called with the correct arguments
        mock_get_execution_conversion_ratio.assert_called_once_with("instance_name", "region_name")

    def test_get_execution_conversion_ratio(self):
        # Set up the mocks
        self.carbon_calculator._datacenter_loader.get_average_cpu_power.return_value = 1.0
        self.carbon_calculator._datacenter_loader.get_average_memory_power.return_value = 2.0
        self.carbon_calculator._datacenter_loader.get_cfe.return_value = 0.5
        self.carbon_calculator._datacenter_loader.get_pue.return_value = 3.0
        self.carbon_calculator._carbon_loader.get_grid_carbon_intensity.return_value = 4.0
        self.carbon_calculator._workflow_loader.get_vcpu.return_value = 5.0
        self.carbon_calculator._workflow_loader.get_memory.return_value = 6.0

        # Call the method
        result = self.carbon_calculator._get_execution_conversion_ratio("instance_name", "provider:region_name")

        # Check that the result is correct
        self.assertEqual(result, (1.0 * 5.0 / 3600, 2.0 * 6.0 / 3600 / 1024, (1 - 0.5) * 3.0 * 4.0))

        # Check that the mocks were called with the correct arguments
        self.carbon_calculator._datacenter_loader.get_average_cpu_power.assert_called_once_with("provider:region_name")
        self.carbon_calculator._datacenter_loader.get_average_memory_power.assert_called_once_with(
            "provider:region_name"
        )
        self.carbon_calculator._datacenter_loader.get_cfe.assert_called_once_with("provider:region_name")
        self.carbon_calculator._datacenter_loader.get_pue.assert_called_once_with("provider:region_name")
        self.carbon_calculator._carbon_loader.get_grid_carbon_intensity.assert_called_once_with(
            "provider:region_name", self.carbon_calculator._hourly_carbon_setting
        )
        self.carbon_calculator._workflow_loader.get_vcpu.assert_called_once_with("instance_name", "provider")
        self.carbon_calculator._workflow_loader.get_memory.assert_called_once_with("instance_name", "provider")

        # Check that the _execution_conversion_ratio_cache attribute was updated correctly
        self.assertEqual(
            self.carbon_calculator._execution_conversion_ratio_cache, {"instance_name_provider:region_name": result}
        )

    @patch.dict(
        "multi_x_serverless.routing.deployment_input.components.calculators.carbon_calculator.__dict__",
        {"CARBON_TRANSMISSION_CARBON_METHOD": "distance"},
    )
    @patch.object(CarbonCalculator, "_get_transmission_conversion_ratio")
    def test_calculate_transmission_carbon_distance(self, mock_get_transmission_conversion_ratio):
        # Set up the mock
        mock_get_transmission_conversion_ratio.return_value = 2.0

        # Call the method
        result = self.carbon_calculator.calculate_transmission_carbon("from_region_name", "to_region_name", 3.0)

        # Check that the result is correct
        self.assertEqual(result, 3.0 * 2.0)

    @patch.dict(
        "multi_x_serverless.routing.deployment_input.components.calculators.carbon_calculator.__dict__",
        {"CARBON_TRANSMISSION_CARBON_METHOD": "latency"},
    )
    @patch.object(CarbonCalculator, "_get_transmission_conversion_ratio")
    def test_calculate_transmission_carbon_latency(self, mock_get_transmission_conversion_ratio):
        # Set up the mock
        mock_get_transmission_conversion_ratio.return_value = 1.0

        # Call the method
        result = self.carbon_calculator.calculate_transmission_carbon("from_region_name", "to_region_name", 3.0)

        # Check that the result is correct
        self.assertEqual(result, 3.0)

    def test_get_transmission_conversion_ratio(self):
        # Set up the mocks
        self.carbon_calculator._carbon_loader.get_transmission_distance.return_value = 1.0
        self.carbon_calculator._carbon_loader.get_grid_carbon_intensity.return_value = 2.0

        # Call the method
        result = self.carbon_calculator._get_transmission_conversion_ratio("from_region_name", "to_region_name")

        # Check that the result is correct
        self.assertEqual(result, 2.0 * (7.564e-6 * 1.0 + 5.762e-3))

        # Check that the mocks were called with the correct arguments
        self.carbon_calculator._carbon_loader.get_transmission_distance.assert_called_once_with(
            "from_region_name", "to_region_name"
        )
        self.carbon_calculator._carbon_loader.get_grid_carbon_intensity.assert_any_call(
            "from_region_name", self.carbon_calculator._hourly_carbon_setting
        )
        self.carbon_calculator._carbon_loader.get_grid_carbon_intensity.assert_any_call(
            "to_region_name", self.carbon_calculator._hourly_carbon_setting
        )

        # Check that the _transmission_conversion_ratio_cache attribute was updated correctly
        self.assertEqual(
            self.carbon_calculator._transmission_conversion_ratio_cache, {"from_region_name_to_region_name": result}
        )


if __name__ == "__main__":
    unittest.main()
