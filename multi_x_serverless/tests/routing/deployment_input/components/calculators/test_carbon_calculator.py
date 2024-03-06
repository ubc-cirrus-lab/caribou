import unittest
from unittest.mock import Mock, patch

import numpy as np
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

    def test_calculate_execution_carbon_distribution(self):
        self.runtime_calculator.calculate_runtime_distribution.return_value = np.array([3600, 2 * 3600, 3 * 3600])
        self.datacenter_loader.get_average_cpu_power.return_value = 1.0
        self.datacenter_loader.get_average_memory_power.return_value = 1.0
        self.datacenter_loader.get_cfe.return_value = 0.5
        self.datacenter_loader.get_pue.return_value = 1.5
        self.carbon_loader.get_grid_carbon_intensity.return_value = 0.5
        self.workflow_loader.get_vcpu.return_value = 2.0
        self.workflow_loader.get_memory.return_value = 4.0

        result = self.carbon_calculator.calculate_execution_carbon_distribution("instance1", "provider1:region1")
        expected_result = np.array([2.25, 4.5, 6.75])
        np.testing.assert_array_equal(result, expected_result)

    def test_calculate_transmission_carbon_distribution(self):
        self.workflow_loader.get_data_transfer_size_distribution.return_value = [1, 2]
        self.runtime_calculator.calculate_latency_distribution.return_value = np.array([1 * 3600, 2 * 3600, 3 * 3600])
        self.carbon_loader.get_transmission_carbon_intensity.return_value = (0.5, 100)

        result = self.carbon_calculator.calculate_transmission_carbon_distribution(
            "instance1", "instance2", "provider1:region1", "provider1:region2"
        )
        expected_result = np.array([0.3, 0.6])
        np.testing.assert_array_equal(result, expected_result)

        # Now for the case of CARBON_TRANSMISSION_CARBON_METHOD is set to "latency"
        with patch(
            "multi_x_serverless.routing.deployment_input.components.calculators.carbon_calculator.CARBON_TRANSMISSION_CARBON_METHOD",
            "latency",
        ):
            result = self.carbon_calculator.calculate_transmission_carbon_distribution(
                "instance1", "instance2", "provider1:region1", "provider1:region2"
            )
            expected_result = np.array([9.05, 18.05, 18.1, 27.05, 36.1, 54.1])
            np.testing.assert_array_equal(result, expected_result)


if __name__ == "__main__":
    unittest.main()
