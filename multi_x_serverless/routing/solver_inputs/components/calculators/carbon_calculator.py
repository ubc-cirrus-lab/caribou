from .calculator import Calculator

import numpy as np

class CarbonCalculator(Calculator):
    def __init__(self):
        super().__init__()
    
    def calculate_execution_carbon(self, compute_configuration: (float, float), execution_time: float, grid_co2e: float, pue: float, cfe: float, compute_kwh: float, memory_kwh_mb: float) -> float:
        memory, vcpu = compute_configuration # Memory in MB

        # # Average power from compute
        # # Compute Watt-Hours = Average Watts * vCPU Hours
        # # GCP: Median Min Watts: 0.71 Median Max Watts: 4.26
        # # In terms of kW
        # average_kw_compute = (0.71 + 0.5 * (4.26 - 0.71)) / 1000
        # vcpu = function_spec["resource_request"]["vCPU"]
        # compute_kwh = average_kw_compute * vcpu * runtime_in_hours

        # # They used 0.000392 Kilowatt Hour / Gigabyte Hour (0.000392 kWh/Gbh) -> 0.000000392 kWh/Mb
        # memory_kw_mb = 0.000000392
        # memory = function_spec["resource_request"]["memory"]  # MB
        # memory_kwh = memory_kw_mb * memory * runtime_in_hours

        # cloud_provider_usage_kwh = compute_kwh + memory_kwh

        # operational_emission = (
        #     cloud_provider_usage_kwh
        #     * (1 - datacenter_data["cfe"])
        #     * datacenter_data["pue"]
        #     * grid_co2_data["carbon_intensity"]
        # )

        # return operational_emission
    
        return 0.0

    def calculate_transmission_carbon(self, ingress_egress_cost: float, data_transfer_size: float) -> float:
        # Both in units of gb
        return ingress_egress_cost * data_transfer_size