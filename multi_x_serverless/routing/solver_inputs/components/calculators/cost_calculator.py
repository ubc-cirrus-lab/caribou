from .calculator import Calculator

import numpy as np

class CostCalculator(Calculator):
    def __init__(self):
        super().__init__()
    
    def calculate_execution_cost(self, compute_cost_information: dict(str, list[(float, int)]), compute_configuration: (float, float), execution_time: float) -> float:
        # TODO (#32): Implement this function
        return 0.0

        #  dict(str, list[(float, int)])

    
    # def calculate_transmission(self, from_instance_index: int, to_instance_index: int, from_region_index: int, to_region_index: int) -> float:
    #     # TODO (#32): Implement this function
    #     return 0.0