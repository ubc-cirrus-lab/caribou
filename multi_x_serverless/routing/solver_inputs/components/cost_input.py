from .input import Input

from .data_sources.data_source_manager import DataSourceManager
from .calculators.cost_calculator import CostCalculator

import numpy as np

class CostInput(Input):
    def __init__(self):
        super().__init__()
        self._cost_calculator = CostCalculator()

    def setup(self, instances_indicies: list[int], regions_indicies: list[int], data_source_manager: DataSourceManager) -> None:
        super().setup()

        # Save the data source manager
        self._data_source_manager = data_source_manager

        # Setup Execution matrix
        self._execution_matrix = np.zeros((len(regions_indicies), len(instances_indicies)))
        for region_index in regions_indicies:
            provider_name: str = data_source_manager.get_region_data("provider_name", region_index) # This is a string
            compute_cost_information: list[(float, int)] = data_source_manager.get_region_data("compute_costs", region_index) # This is a list
            # free_tier = data_source_manager.get_region_data("free_tier", region_index) # Free tier not yet implemented

            for instance_index in instances_indicies:
                execution_time: float = data_source_manager.get_instance_data("execution_time", instance_index) # This is a value in seconds

                # Compute information aquisition
                provider_configuration: dict = data_source_manager.get_instance_data("provider_configurations", instance_index)
                compute_configuration = provider_configuration.get(provider_name, None)

                # Calculate final value
                if (compute_configuration is not None):
                    # Basically some instances are not available in some regions -> so we just ignore them
                    self._execution_matrix[region_index][instance_index] = self._cost_calculator.calculate_execution_cost(compute_cost_information, compute_configuration, execution_time)
        
        # Setup Transmission matrix
        # Here it is more complex as we need to first consider region to region transmission
        # Then relate this to instance to instance transmission information
        
        # Lets first setup the region_to_region information matrix first
        # Here cost is simply ingress + egress (Where ingress is normally 0 -> but it seems like google is different)
        self._region_to_region_matrix = np.zeros((len(regions_indicies), len(regions_indicies)))
        for from_region_index in regions_indicies:
            for to_region_index in regions_indicies:
                ingress_cost = data_source_manager.get_region_to_region_data("data_transfer_ingress_cost", from_region_index, to_region_index)
                egress_cost = data_source_manager.get_region_to_region_data("data_transfer_egress_cost", from_region_index, to_region_index)

                self._region_to_region_matrix[from_region_index][to_region_index] = self._cost_calculator.calculate_transmission_cost_per_gb(ingress_cost, egress_cost)

    def get_transmission_value(self, from_instance_index: int, to_instance_index: int, from_region_index: int, to_region_index: int) -> float:
        # Handle special cases of from and to nothing (Basically start at 0, end at 0)
        if from_region_index is None:
            return 0 # So nothing was moved, no co2e

        # We simply use this to get the data transfer size and calculate total cost
        transmission_size = self._data_source_manager.get_instance_to_instance_data("data_transfer_size", from_instance_index, to_instance_index)
        transmission_cost_per_gb = self._region_to_region_matrix[from_region_index][to_region_index]

        return self._cost_calculator.calculate_transmission_cost(transmission_cost_per_gb, transmission_size)