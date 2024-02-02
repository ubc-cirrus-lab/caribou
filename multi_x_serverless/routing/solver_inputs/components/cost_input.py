import numpy as np

from multi_x_serverless.routing.solver_inputs.components.calculators.cost_calculator import CostCalculator
from multi_x_serverless.routing.solver_inputs.components.data_sources.data_source_manager import DataSourceManager
from multi_x_serverless.routing.solver_inputs.components.input import Input
from multi_x_serverless.routing.solver_inputs.components.runtime_input import RuntimeInput


class CostInput(Input):
    def __init__(self) -> None:
        self._cost_calculator = CostCalculator()
        self._data_transfer_size_matrix = np.zeros((0, 0), dtype=float)
        self._transmission_cost_matrix = np.zeros((0, 0), dtype=float)

    def setup(
        self,
        instances_indicies: list[int],
        regions_indicies: list[int],
        data_source_manager: DataSourceManager,
        runtime_input: RuntimeInput,
    ) -> None:
        # Save the data source manager
        self._data_source_manager = data_source_manager

        # Setup Execution matrix
        self._execution_matrix = np.zeros((len(regions_indicies), len(instances_indicies)), dtype=float)
        for region_index in regions_indicies:
            provider_name: str = data_source_manager.get_region_data("provider_name", region_index)
            compute_cost: float = data_source_manager.get_region_data("compute_cost", region_index)

            for instance_index in instances_indicies:
                execution_time: float = runtime_input.get_execution_value(instance_index, region_index, False)

                # Compute information aquisition
                provider_configuration: dict[str, dict[str, float]] = dict(
                    data_source_manager.get_instance_data("provider_configurations", instance_index)
                )
                compute_configuration: dict[str, float] = provider_configuration.get(provider_name, {})

                # Calculate final value
                if compute_configuration:
                    # Basically some instances are not available in some regions -> so we just ignore them
                    self._execution_matrix[region_index][
                        instance_index
                    ] = self._cost_calculator.calculate_execution_cost(
                        compute_cost, compute_configuration, execution_time
                    )

        # Matricies for calculating transmission
        self._transmission_cost_matrix = np.zeros((len(regions_indicies), len(regions_indicies)), dtype=float)
        for from_region_index in regions_indicies:
            for to_region_index in regions_indicies:
                ingress_cost = float(
                    data_source_manager.get_region_to_region_data(
                        "data_transfer_ingress_cost", from_region_index, to_region_index
                    )
                )
                egress_cost = float(
                    data_source_manager.get_region_to_region_data(
                        "data_transfer_egress_cost", from_region_index, to_region_index
                    )
                )

                self._transmission_cost_matrix[from_region_index][
                    to_region_index
                ] = self._cost_calculator.calculate_transmission_cost_per_gb(ingress_cost, egress_cost)

        self._data_transfer_size_matrix = np.zeros((len(instances_indicies), len(instances_indicies)), dtype=float)
        for from_instance_index in instances_indicies:
            for to_instance_index in instances_indicies:
                data_transfer_size = data_source_manager.get_instance_to_instance_data(
                    "data_transfer_size", from_instance_index, to_instance_index
                )
                self._data_transfer_size_matrix[from_instance_index][to_instance_index] = data_transfer_size

    def get_transmission_value(
        self,
        from_instance_index: int,
        to_instance_index: int,
        from_region_index: int,
        to_region_index: int,
        consider_probabilistic_invocations: bool,
    ) -> float:
        # Handle special cases of from and to nothing (Basically start at 0, end at 0)
        if from_region_index is None:
            return 0  # So nothing was moved, no co2e

        # We simply use this to get the data transfer size and calculate total cost
        data_transfer_size: float = float(self._data_transfer_size_matrix[from_instance_index][to_instance_index])
        transmission_cost_per_gb: float = float(self._transmission_cost_matrix[from_region_index][to_region_index])

        return self._cost_calculator.calculate_transmission_cost(transmission_cost_per_gb, data_transfer_size)
