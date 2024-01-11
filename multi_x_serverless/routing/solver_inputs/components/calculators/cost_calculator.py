from .calculator import Calculator

import numpy as np

class CostCalculator(Calculator):
    def __init__(self):
        super().__init__()
    
    def calculate_execution_cost(self, compute_cost_information: list[(float, int)], execution_time: float) -> float:
        # TODO (#32): Implement this function
        return 0.0


    
    # def calculate_transmission(self, from_instance_index: int, to_instance_index: int, from_region_index: int, to_region_index: int) -> float:
    #     # TODO (#32): Implement this function
    #     return 0.0