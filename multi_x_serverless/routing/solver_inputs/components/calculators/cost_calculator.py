from multi_x_serverless.routing.solver_inputs.components.calculators.calculator import Calculator


class CostCalculator(Calculator):
    def calculate_execution_cost(
        self,
        compute_cost: float,
        compute_configuration: dict[str, float],
        execution_time: float,
    ) -> float:
        memory: float = float(compute_configuration["memory"])
        vcpu: float = float(compute_configuration["vcpu"])

        # Compute cost in USD /  GB-seconds
        # Memory in MB, execution_time in seconds, vcpu in vcpu
        memory_gb = memory / 1024
        gbs = memory_gb * vcpu * execution_time
        return compute_cost * gbs

    def calculate_transmission_cost(self, transmission_cost_per_gb: float, transmission_size: float) -> float:
        # Both in units of gb
        return transmission_cost_per_gb * transmission_size

    def calculate_transmission_cost_per_gb(self, ingress_cost: float, egress_cost: float) -> float:
        # Both in units of gb
        return ingress_cost + egress_cost
