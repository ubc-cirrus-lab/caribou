from .components.input import Input

from .components.carbon_input import CarbonInput
from .components.cost_input import CostInput
from .components.runtime_input import RuntimeInput

class InputManager():
    def __init__(self):
        self._carbon_input = CarbonInput()
        self._cost_input = CostInput()
        self._runtime_input = RuntimeInput()

    def get_execution_value(self, desired_calculator: str, instance_index: int, region_index: int) -> float:
        return self._get_input_component_manager(desired_calculator).get_execution_value(instance_index, region_index)

    def get_execution_cost_carbon_runtime(self, instance_index: int, region_index: int) -> float:
        results = []
        calculators = ["Cost", "Carbon", "Runtime"]
        for calculator in calculators:
            results.append(self._get_input_component_manager(calculator).get_execution_value(instance_index, region_index))
        return results

    def calculate_transmission(self, desired_calculator: str, from_instance_index: int, to_instance_index: int, from_region_index: int, to_region_index: int) -> list[float]:
        return self._get_input_component_manager(desired_calculator).get_transmission_value(from_instance_index, to_instance_index, from_region_index, to_region_index)
    
    def calculate_transmission_cost_carbon_runtime(self, from_instance_index: int, to_instance_index: int, from_region_index: int, to_region_index: int) -> list[float]:
        results = []
        calculators = ["Cost", "Carbon", "Runtime"]
        for calculator in calculators:
            results.append(self._get_input_component_manager(calculator).get_transmission_value(from_instance_index, to_instance_index, from_region_index, to_region_index))
        return results

    def _get_input_component_manager(self, desired_calculator: str) -> Input:
        return {
            "Carbon": self._carbon_input,
            "Cost": self._cost_input,
            "Runtime": self._runtime_input,
        }[desired_calculator]