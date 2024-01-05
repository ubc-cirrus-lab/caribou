from multi_x_serverless.routing.data_sources.data_manager import DataManager

from multi_x_serverless.routing.calculations.components.calculator import Calculator
from multi_x_serverless.routing.calculations.components.carbon import CarbonCalculator
from multi_x_serverless.routing.calculations.components.cost import CostCalculator
from multi_x_serverless.routing.calculations.components.runtime import RuntimeCalculator

class CalculationManager():
    def __init__(self, data_manager: DataManager):
        self._carbon_calculator = CarbonCalculator(data_manager)
        self._cost_calculator = CostCalculator(data_manager)
        self._runtime_calculator = RuntimeCalculator(data_manager)

    def calculate_execution(self, desired_calculator: str, instance_index: int, region_index: int) -> float:
        return self._get_calculator(desired_calculator).calculate_execution(instance_index, region_index)

    def calculate_execution_cost_carbon_runtime(self, instance_index: int, region_index: int) -> float:
        results = []
        calculators = ["Cost", "Carbon", "Runtime"]
        for calculator in calculators:
            results.append(self._get_calculator(calculator).calculate_execution(instance_index, region_index))
        return results

    def calculate_transmission(self, desired_calculator: str, from_instance_index: int, to_instance_index: int, from_region_index: int, to_region_index: int) -> float:
        return self._get_calculator(desired_calculator).calculate_transmission(from_instance_index, to_instance_index, from_region_index, to_region_index)
    
    def calculate_transmission_cost_carbon_runtime(self, from_instance_index: int, to_instance_index: int, from_region_index: int, to_region_index: int) -> float:
        results = []
        calculators = ["Cost", "Carbon", "Runtime"]
        for calculator in calculators:
            results.append(self._get_calculator(calculator).calculate_transmission(from_instance_index, to_instance_index, from_region_index, to_region_index))
        return results

    def _get_calculator(self, desired_calculator: str) -> Calculator:
        return {
            "Carbon": self._carbon_calculator,
            "Cost": self._cost_calculator,
            "Runtime": self._runtime_calculator,
        }[desired_calculator]