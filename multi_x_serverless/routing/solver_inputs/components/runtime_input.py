import numpy as np

from multi_x_serverless.routing.solver_inputs.components.calculators.runtime_calculator import RuntimeCalculator
from multi_x_serverless.routing.solver_inputs.components.data_sources.data_source_manager import DataSourceManager
from multi_x_serverless.routing.solver_inputs.components.input import Input


class RuntimeInput(Input):
    def __init__(self) -> None:
        super().__init__()
        self._runtime_calculator = RuntimeCalculator()

    def setup(
        self, instances_indicies: list[int], regions_indicies: list[int], data_source_manager: DataSourceManager
    ) -> None:
        self._cache = {}

        self._data_source_manager = data_source_manager

        # Setup Execution matrix
        self._execution_matrix = np.zeros((len(regions_indicies), len(instances_indicies)), dtype=float)
        for region_index in regions_indicies:
            for instance_index in instances_indicies:
                execution_time: float = data_source_manager.get_instance_data("execution_time", instance_index)

                # TODO: Need to consider region differences

                # Calculate final value
                self._execution_matrix[region_index][instance_index] = execution_time

    def get_transmission_value(
        self, from_instance_index: int, to_instance_index: int, from_region_index: int, to_region_index: int
    ) -> float:
        # Handle special cases of from and to nothing (Basically start at 0, end at 0)
        if from_region_index is None:
            return 0  # So nothing was moved, no latency

        # We simply use this to get the data transfer size and calculate total cost
        transmission_size = self._data_source_manager.get_instance_to_instance_data(
            "data_transfer_size", from_instance_index, to_instance_index
        )
        transmission_times = self._data_source_manager.get_region_to_region_data(
            "transmission_times", from_region_index, to_region_index
        )

        return self._runtime_calculator.calculate_transmission_latency(transmission_times, transmission_size)
