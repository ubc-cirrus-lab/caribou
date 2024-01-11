from .calculator import Calculator

import numpy as np

class RuntimeCalculator(Calculator):
    def __init__(self):
        super().__init__()
    
    def calculate_transmission_latency(self, transmission_times: list[(float, float)], transmission_size: float) -> float:
        # Both in units of gb
        
        
        return 0.0