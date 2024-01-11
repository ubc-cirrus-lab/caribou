from .input import Input

from .data_sources.data_source_manager import DataSourceManager
from .calculators.cost_calculator import CostCalculator

import numpy as np

class CostInput(Input):
    def __init__(self):
        super().__init__()
        self._cost_calculator = CostCalculator()

    def setup(self, instances_indicies: list[int], regions_indicies: list[int], data_source_manager: DataSourceManager) -> None:
        super().setup()

        self._data_source_manager = data_source_manager

        # Setup Execution matrix
        self._execution_matrix = np.zeros((len(regions_indicies), len(instances_indicies)))
        for region_index in regions_indicies:
            compute_cost_information = data_source_manager.get_region_data("compute_cost", region_index) # This is a list
            # free_tier = data_source_manager.get_region_data("free_tier", region_index) # Free tier not yet implemented
            for instance_index in instances_indicies:
                execution_time = data_source_manager.d("execution_time", instance_index) # This is a value in seconds

                # compute_cost_information: dict(str, list[(float, int)]), provider_name: str, execution_time: float
                self._execution_matrix[region_index][instance_index] = self._cost_calculator.calculate_execution_cost(compute_cost_information, execution_time)
        
        # Setup Transmission matrix
        # Here it is more complex as we need to first consider region to region transmission
        # Then relate this to instance to instance transmission information
        

        self._instances_indicies = instances_indicies
        self._regions_indicies = regions_indicies
    
    def get_transmission_value(self, from_instance_index: int, to_instance_index: int, from_region_index: int, to_region_index: int) -> float:
        return 0.0