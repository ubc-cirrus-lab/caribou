from multi_x_serverless.routing.calculations.components.calculator import Calculator
from multi_x_serverless.routing.data_sources.data_manager import DataManager

import numpy as np

class CostCalculator(Calculator):
    def __init__(self, data_manager: DataManager):
        super().__init__(data_manager)

    def calculate_execution(self, instance_index: int, region_index: int) -> float:
        # TODO (#32): Implement this function
        return 0.0

    def calculate_transmission(self, from_instance_index: int, to_instance_index: int, from_region_index: int, to_region_index: int) -> float:
        # TODO (#32): Implement this function
        return 0.0