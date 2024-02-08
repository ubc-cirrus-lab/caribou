from multi_x_serverless.routing.solver_inputs.components.calculators.calculator import Calculator


class RuntimeCalculator(Calculator):
    def calculate_execution_time(self, transmission_times: float, runtime_scaling_factor: float) -> float:
        return transmission_times * runtime_scaling_factor

    def calculate_transmission_latency(
        self, transmission_times: list[tuple[float, float]], current_transmission_size: float
    ) -> float:
        # Both in units of gb
        if not transmission_times or len(transmission_times) == 0:  # No information
            return -1

        worse_case_latency = transmission_times[-1][
            1
        ]  # Assume at index 1 is the latency in seconds of worse case scenerio

        for transmission_time in transmission_times:
            test_transmitted_data = transmission_time[0]
            if current_transmission_size > test_transmitted_data:
                continue
            return transmission_time[1]

        return worse_case_latency
