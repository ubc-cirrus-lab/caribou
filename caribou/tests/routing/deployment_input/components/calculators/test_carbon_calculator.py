import unittest
from unittest.mock import MagicMock
from caribou.deployment_solver.deployment_input.components.calculators.carbon_calculator import CarbonCalculator
from caribou.deployment_solver.deployment_input.components.loaders.carbon_loader import CarbonLoader
from caribou.deployment_solver.deployment_input.components.loaders.datacenter_loader import DatacenterLoader
from caribou.deployment_solver.deployment_input.components.loaders.workflow_loader import WorkflowLoader
from caribou.common.constants import AVERAGE_USA_CARBON_INTENSITY


class TestCarbonCalculator(unittest.TestCase):
    def setUp(self):
        # Create mocks for the loaders
        self.carbon_loader = MagicMock(spec=CarbonLoader)
        self.datacenter_loader = MagicMock(spec=DatacenterLoader)
        self.workflow_loader = MagicMock(spec=WorkflowLoader)

        # Create an instance of CarbonCalculator with mocks
        self.carbon_calculator = CarbonCalculator(
            carbon_loader=self.carbon_loader,
            datacenter_loader=self.datacenter_loader,
            workflow_loader=self.workflow_loader,
            energy_factor_of_transmission=0.001,
            carbon_free_intra_region_transmission=False,
            carbon_free_dt_during_execution_at_home_region=False,
            consider_cfe=True,
        )

    def test_alter_carbon_setting(self):
        # Set a new carbon setting
        new_setting = "hourly"

        # Call the method
        self.carbon_calculator.alter_carbon_setting(new_setting)

        # Check that the carbon setting and caches are updated correctly
        self.assertEqual(self.carbon_calculator._hourly_carbon_setting, new_setting)
        self.assertEqual(self.carbon_calculator._execution_conversion_ratio_cache, {})
        self.assertEqual(self.carbon_calculator._transmission_conversion_ratio_cache, {})

    def test_calculate_virtual_start_instance_carbon(self):
        # Define the test data
        data_input_sizes = {"aws:us-west-2": 10.0}
        data_output_sizes = {"aws:us-east-1": 5.0}

        # Mock the data transfer carbon calculation
        self.carbon_calculator._calculate_data_transfer_carbon = MagicMock(return_value=0.5)

        # Call the method under test
        carbon = self.carbon_calculator.calculate_virtual_start_instance_carbon(data_input_sizes, data_output_sizes)

        # Assert the carbon calculation is correct
        self.assertEqual(carbon, 0.5)

        # Assert that the private method was called with the correct arguments
        self.carbon_calculator._calculate_data_transfer_carbon.assert_called_once_with(
            None, data_input_sizes, data_output_sizes, 0.0
        )

    def test_calculate_instance_carbon(self):
        # Define the test data
        execution_time = 100.0
        instance_name = "test_instance"
        region_name = "aws:us-west-2"
        data_input_sizes = {"aws:us-east-1": 5.0}
        data_output_sizes = {"aws:us-east-1": 10.0}
        data_transfer_during_execution = 2.0
        is_invoked = True
        is_redirector = False

        # Mock the execution and data transfer carbon calculations
        self.carbon_calculator._calculate_execution_carbon = MagicMock(return_value=1.0)
        self.carbon_calculator._calculate_data_transfer_carbon = MagicMock(return_value=0.5)

        # Call the method under test
        execution_carbon, transmission_carbon = self.carbon_calculator.calculate_instance_carbon(
            execution_time,
            instance_name,
            region_name,
            data_input_sizes,
            data_output_sizes,
            data_transfer_during_execution,
            is_invoked,
            is_redirector,
        )

        # Assert the carbon calculations are correct
        self.assertEqual(execution_carbon, 1.0)
        self.assertEqual(transmission_carbon, 0.5)

        # Assert that the private methods were called with the correct arguments
        self.carbon_calculator._calculate_execution_carbon.assert_called_once_with(
            instance_name, region_name, execution_time, is_redirector
        )
        self.carbon_calculator._calculate_data_transfer_carbon.assert_called_once_with(
            region_name, data_input_sizes, data_output_sizes, data_transfer_during_execution
        )

    def test_calculate_data_transfer_carbon(self):
        # Define the test data
        current_region_name = "aws:us-west-2"
        data_input_sizes = {"aws:us-east-1": 5.0, "aws:us-west-2": 2.0}
        data_output_sizes = {"aws:us-east-1": 10.0}  # Irrelevant
        data_transfer_during_execution = 3.0

        # Mock the carbon intensity retrieval
        self.carbon_loader.get_grid_carbon_intensity.return_value = 0.2
        self.workflow_loader.get_home_region.return_value = "aws:us-east-1"

        # Call the private method under test
        carbon = self.carbon_calculator._calculate_data_transfer_carbon(
            current_region_name, data_input_sizes, data_output_sizes, data_transfer_during_execution
        )

        # Calculate expected carbon
        expected_carbon = (
            (5.0 * 0.001 * AVERAGE_USA_CARBON_INTENSITY)
            + (2.0 * 0.001 * 0.2)
            + (3.0 * 0.001 * AVERAGE_USA_CARBON_INTENSITY)
        )

        # Assert the carbon calculation is correct
        self.assertEqual(carbon, expected_carbon)

    def test_calculate_execution_carbon(self):
        # Define the test data
        instance_name = "test_instance"
        region_name = "aws:us-west-2"
        execution_latency = 50.0
        is_redirector = False

        # Mock the execution conversion ratio retrieval
        self.carbon_calculator._get_execution_conversion_ratio = MagicMock(return_value=(0.1, 0.2, 0.3))

        # Call the private method under test
        carbon = self.carbon_calculator._calculate_execution_carbon(
            instance_name, region_name, execution_latency, is_redirector
        )

        # Calculate expected carbon
        expected_carbon = execution_latency * (0.1 + 0.2) * 0.3

        # Assert the carbon calculation is correct
        self.assertEqual(carbon, expected_carbon)

    def test_get_execution_conversion_ratio(self):
        # Define the test data
        instance_name = "test_instance"
        region_name = "aws:us-west-2"
        is_redirector = False

        # Mock the data retrieval methods
        self.datacenter_loader.get_average_memory_power.return_value = 0.5
        self.datacenter_loader.get_cfe.return_value = 0.2
        self.datacenter_loader.get_pue.return_value = 1.1
        self.carbon_loader.get_grid_carbon_intensity.return_value = 0.3
        self.workflow_loader.get_vcpu.return_value = 2.0
        self.workflow_loader.get_memory.return_value = 4096
        self.datacenter_loader.get_min_cpu_power.return_value = 0.1
        self.datacenter_loader.get_max_cpu_power.return_value = 0.5
        self.workflow_loader.get_average_cpu_utilization.return_value = 0.6

        # Call the private method under test
        compute_factor, memory_factor, power_factor = self.carbon_calculator._get_execution_conversion_ratio(
            instance_name, region_name, is_redirector
        )

        # Calculate expected values
        memory_gb = 4096 / 1024
        utilization = 0.6
        min_cpu_power = 0.1
        max_cpu_power = 0.5
        average_cpu_power = min_cpu_power + utilization * (max_cpu_power - min_cpu_power)
        expected_compute_factor = average_cpu_power * 2.0 / 3600
        expected_memory_factor = 0.5 * memory_gb / 3600
        expected_power_factor = (1 - 0.2) * 1.1 * 0.3

        # Assert the conversion ratios are correct
        self.assertEqual(compute_factor, expected_compute_factor)
        self.assertEqual(memory_factor, expected_memory_factor)
        self.assertEqual(power_factor, expected_power_factor)

        # Assert that the cache was updated correctly
        self.assertEqual(
            self.carbon_calculator._execution_conversion_ratio_cache[f"{instance_name}_{region_name}"],
            (compute_factor, memory_factor, power_factor),
        )


if __name__ == "__main__":
    unittest.main()
