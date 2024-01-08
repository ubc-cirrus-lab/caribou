# Inner Components
from .components.input import Input

from .components.carbon_input import CarbonInput
from .components.cost_input import CostInput
from .components.runtime_input import RuntimeInput

# Outside library
from multi_x_serverless.routing.workflow_config import WorkflowConfig
from multi_x_serverless.routing.models.indexer import Indexer

import numpy as np

class InputManager():
    def __init__(self, config: WorkflowConfig, regions_indexer: Indexer, instance_indexer: Indexer):
        super().__init__()
        self._config = config
        self._regions_indexer = regions_indexer
        self._instance_indexer = instance_indexer

        # initialize 
        self._carbon_input = CarbonInput()
        self._cost_input = CostInput()
        self._runtime_input = RuntimeInput()

    def setup(self, regions: np.ndarray) -> None:
        self._carbon_input.setup(regions, self._config, self._regions_indexer, self._instance_indexer)
        self._cost_input.setup(regions, self._config, self._regions_indexer, self._instance_indexer)
        self._runtime_input.setup(regions, self._config, self._regions_indexer, self._instance_indexer)

    def get_execution_value(self, desired_calculator: str, instance_index: int, region_index: int) -> float:
        return self._get_input_component_manager(desired_calculator).get_execution_value(instance_index, region_index)

    def get_execution_cost_carbon_runtime(self, instance_index: int, region_index: int) -> float:
        results = []
        calculators = ["Cost", "Carbon", "Runtime"]
        for calculator in calculators:
            results.append(self._get_input_component_manager(calculator).get_execution_value(instance_index, region_index))
        return results

    def get_transmission(self, desired_calculator: str, from_instance_index: int, to_instance_index: int, from_region_index: int, to_region_index: int) -> list[float]:
        return self._get_input_component_manager(desired_calculator).get_transmission_value(from_instance_index, to_instance_index, from_region_index, to_region_index)
    
    def get_transmission_cost_carbon_runtime(self, from_instance_index: int, to_instance_index: int, from_region_index: int, to_region_index: int) -> list[float]:
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