import unittest
from unittest.mock import Mock
from multi_x_serverless.routing.solver.input.components.calculators.carbon_calculator import CarbonCalculator

class TestCarbonCalculator(unittest.TestCase):
    def setUp(self):
        self.carbon_loader = Mock()
        self.datacenter_loader = Mock()
        self.workflow_loader = Mock()
        self.runtime_calculator = Mock()
        self.carbon_calculator = CarbonCalculator(
            self.carbon_loader,
            self.datacenter_loader,
            self.workflow_loader,
            self.runtime_calculator
        )

    def test_calculate_execution_carbon(self):
        self.carbon_calculator._calculate_raw_execution_carbon = Mock(return_value=10.0)
        result = self.carbon_calculator.calculate_execution_carbon('instance1', 'region1')
        self.assertEqual(result, 10.0)

    def test_calculate_transmission_carbon(self):
        self.carbon_calculator._calculate_raw_transmission_carbon = Mock(return_value=20.0)
        result = self.carbon_calculator.calculate_transmission_carbon('instance1', 'instance2', 'region1', 'region2')
        self.assertEqual(result, 20.0)

    def test__calculate_raw_execution_carbon(self):
        self.runtime_calculator.calculate_raw_runtime.return_value = 3600
        self.datacenter_loader.get_average_cpu_power.return_value = 1
        self.datacenter_loader.get_average_memory_power.return_value = 1
        self.datacenter_loader.get_cfe.return_value = 0.5
        self.datacenter_loader.get_pue.return_value = 1.5
        self.carbon_loader.get_grid_carbon_intensity.return_value = 0.5
        self.workflow_loader.get_vcpu.return_value = 2
        self.workflow_loader.get_memory.return_value = 4

        result = self.carbon_calculator._calculate_raw_execution_carbon("instance1", "provider1:region1")
        self.assertEqual(result, 2.25)

    def test__calculate_raw_transmission_carbon(self):
        self.workflow_loader.get_data_transfer_size.return_value = 2
        self.carbon_loader.get_transmission_carbon_intensity.return_value = 0.5

        result = self.carbon_calculator._calculate_raw_transmission_carbon(
            "instance1", "instance2", "provider1:region1", "provider1:region2"
        )
        self.assertEqual(result, 1)

if __name__ == '__main__':
    unittest.main()