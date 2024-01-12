import numpy as np

from .calculator import Calculator


class RuntimeCalculator(Calculator):
    def __init__(self):
        super().__init__()

    def calculate_execution_time(
        self, execution_times: list[(float, int)], compute_configuration: (float, float)
    ) -> float:
        # TODO: Need to consider performance conversion between both within regions and across providers
        None

    def calculate_transmission_latency(
        self, transmission_times: list[(float, float)], current_transmission_size: float
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
            else:
                return transmission_time[1]

        return worse_case_latency
