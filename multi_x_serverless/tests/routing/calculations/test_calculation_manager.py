import unittest
from unittest.mock import Mock, patch
from multi_x_serverless.routing.calculations.components.cost import CostCalculator
from multi_x_serverless.routing.calculations.components.runtime import RuntimeCalculator
from multi_x_serverless.routing.calculations.components.carbon import CarbonCalculator
from multi_x_serverless.routing.calculations.calculation_manager import CalculationManager

from multi_x_serverless.routing.data_sources.data_manager import DataManager

# TODO: Expand this to actually test the calculations instead of just the calls
class TestCalculationManager(unittest.TestCase):
    @patch('multi_x_serverless.routing.data_sources.data_manager.DataManager')
    @patch('multi_x_serverless.routing.calculations.components.carbon.CarbonCalculator')
    @patch('multi_x_serverless.routing.calculations.components.cost.CostCalculator')
    @patch('multi_x_serverless.routing.calculations.components.runtime.RuntimeCalculator')
    def setUp(self, mock_runtime_calculator, mock_cost_calculator, mock_carbon_calculator, mock_data_manager):
        self.data_manager = mock_data_manager.return_value
        self.carbon_calculator = mock_carbon_calculator.return_value
        self.cost_calculator = mock_cost_calculator.return_value
        self.runtime_calculator = mock_runtime_calculator.return_value

        self.calculation_manager = CalculationManager(self.data_manager)

        self.calculation_manager._carbon_calculator = self.carbon_calculator
        self.calculation_manager._cost_calculator = self.cost_calculator
        self.calculation_manager._runtime_calculator = self.runtime_calculator

    def test_calculate_execution(self):
        self.calculation_manager.calculate_execution("Cost", 0, 0)
        self.cost_calculator.calculate_execution.assert_called_once_with(0, 0)

    def test_calculate_execution_cost_carbon_runtime(self):
        self.calculation_manager.calculate_execution_cost_carbon_runtime(0, 0)
        self.cost_calculator.calculate_execution.assert_called_once_with(0, 0)
        self.carbon_calculator.calculate_execution.assert_called_once_with(0, 0)
        self.runtime_calculator.calculate_execution.assert_called_once_with(0, 0)

    def test_calculate_transmission(self):
        self.calculation_manager.calculate_transmission("Cost", 0, 1, 0, 1)
        self.cost_calculator.calculate_transmission.assert_called_once_with(0, 1, 0, 1)

    def test_calculate_transmission_cost_carbon_runtime(self):
        self.calculation_manager.calculate_transmission_cost_carbon_runtime(0, 1, 0, 1)
        self.cost_calculator.calculate_transmission.assert_called_once_with(0, 1, 0, 1)
        self.carbon_calculator.calculate_transmission.assert_called_once_with(0, 1, 0, 1)
        self.runtime_calculator.calculate_transmission.assert_called_once_with(0, 1, 0, 1)

if __name__ == '__main__':
    unittest.main()