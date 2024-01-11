from .calculator import Calculator

import numpy as np

class CostCalculator(Calculator):
    def __init__(self):
        super().__init__()
    
    def calculate_execution_cost(self, compute_cost_information: list[(float, int)], compute_configuration: (float, float), execution_time: float) -> float:
        # TODO (#32): Implement this function
        memory, vcpu = compute_configuration # Memory in MB

        return 0.0
    
    def calculate_transmission_cost(self, ingress_egress_cost: float, data_transfer_size: float) -> float:
        # Both in units of gb
        return ingress_egress_cost * data_transfer_size


    def calculate_transmission_cost_per_gb(self, ingress_cost: float, egress_cost: float) -> float:
        # Both in units of gb
        return ingress_cost + egress_cost