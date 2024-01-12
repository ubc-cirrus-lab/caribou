from .calculator import Calculator

import numpy as np

class CostCalculator(Calculator):
    def __init__(self):
        super().__init__()
    
    def calculate_execution_cost(self, compute_cost_information: list[(float, int)], compute_configuration: (float, float), execution_time: float) -> float:
        # TODO (#32): Implement this function
        memory, vcpu = compute_configuration # Memory in MB
        
        # print("\n\nNew stuff:", compute_cost_information, compute_configuration, execution_time)
        
        # if estimated_gb_seconds_per_month <= compute_free_tier:
        #     return compute_cost

        # estimated_gb_seconds_per_month = (
        #         estimated_memory * estimated_duration * estimated_number_of_requests_per_month
        #     )
        
        # estimated_gb_seconds_per_month -= compute_free_tier

        # for price_dimension in price_dimensions.values():
        #     if estimated_gb_seconds_per_month <= int(price_dimension["endRange"]):
        #         compute_cost += float(price_dimension["pricePerUnit"]["USD"]) * estimated_gb_seconds_per_month
        #         break
        #     compute_cost += float(price_dimension["pricePerUnit"]["USD"]) * int(price_dimension["endRange"])
        #     estimated_gb_seconds_per_month -= int(price_dimension["endRange"])
        # return compute_cost
        
        return 0.0
    
    def calculate_transmission_cost(self, transmission_cost_per_gb: float, transmission_size: float) -> float:
        # Both in units of gb
        return transmission_cost_per_gb * transmission_size


    def calculate_transmission_cost_per_gb(self, ingress_cost: float, egress_cost: float) -> float:
        # Both in units of gb
        return ingress_cost + egress_cost