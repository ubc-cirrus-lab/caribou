from .input import Input

from .data_sources.data_source_manager import DataSourceManager

from .calculators.cost_calculator import CostCalculator

import numpy as np

class RuntimeInput(Input):
    def __init__(self):
        super().__init__()
        self._cost_calculator = CostCalculator()

    def setup(self, instances_indicies: list[int], regions_indicies: list[int], data_source_manager: DataSourceManager) -> None:
        super().setup()

        self._data_source_manager = data_source_manager

        self._instances_indicies = instances_indicies
        self._regions_indicies = regions_indicies
    
    def get_transmission_value(self, from_instance_index: int, to_instance_index: int, from_region_index: int, to_region_index: int) -> float:
        return 0.0