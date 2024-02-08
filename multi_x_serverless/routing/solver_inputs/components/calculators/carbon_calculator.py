from multi_x_serverless.routing.solver_inputs.components.calculators.calculator import Calculator


class CarbonCalculator(Calculator):
    def calculate_execution_carbon(
        self,
        compute_configuration: dict[str, float],
        execution_time: float,
        grid_co2e: float,
        pue: float,
        cfe: float,
        average_kw_compute: float,
        memory_kw_mb: float,
    ) -> float:
        memory: float = float(compute_configuration["memory"])
        vcpu: float = float(compute_configuration["vcpu"])

        # Average power from compute
        # Compute Watt-Hours = Average Watts * vCPU Hours
        runtime_in_hours = execution_time / 3600  # Seconds to hours
        compute_kwh = average_kw_compute * vcpu * runtime_in_hours

        memory_kwh = memory_kw_mb * memory * runtime_in_hours

        cloud_provider_usage_kwh = compute_kwh + memory_kwh

        operational_emission = cloud_provider_usage_kwh * (1 - cfe) * pue * grid_co2e

        return operational_emission

    def calculate_transmission_carbon(self, transmission_co2e_per_gb: float, data_transfer_size: float) -> float:
        # Both in units of gb
        return transmission_co2e_per_gb * data_transfer_size
