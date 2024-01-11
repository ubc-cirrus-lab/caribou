from .input import Input

from .data_sources.data_source_manager import DataSourceManager
from .calculators.carbon_calculator import CarbonCalculator

import numpy as np

class CarbonInput(Input):
    def __init__(self):
        super().__init__()
        self._carbon_calculator = CarbonCalculator()

    def setup(self, instances_indicies: list[int], regions_indicies: list[int], data_source_manager: DataSourceManager) -> None:
        super().setup()

        # Save the data source manager
        self._data_source_manager = data_source_manager

        # Setup Execution matrix
        self._execution_matrix = np.zeros((len(regions_indicies), len(instances_indicies)))
        for region_index in regions_indicies:
            provider_name: str = data_source_manager.get_region_data("provider_name", region_index) # This is a string

            # Co2e information
            grid_co2e: float = data_source_manager.get_region_data("grid_co2e", region_index)
            pue: float = data_source_manager.get_region_data("pue", region_index)
            cfe: float = data_source_manager.get_region_data("cfe", region_index)
            compute_kwh: float = data_source_manager.get_region_data("compute_kwh", region_index)
            memory_kwh_mb: float = data_source_manager.get_region_data("memory_kwh_mb", region_index)

            for instance_index in instances_indicies:
                execution_time: float = data_source_manager.get_instance_data("execution_time", instance_index) # This is a value in seconds

                # Compute information aquisition
                provider_configuration: dict = data_source_manager.get_instance_data("provider_configurations", instance_index)
                compute_configuration = provider_configuration.get(provider_name, None)

                # Calculate final value
                self._execution_matrix[region_index][instance_index] = self._carbon_calculator.calculate_execution_carbon(
                    compute_configuration, execution_time,
                    grid_co2e, pue, cfe, compute_kwh, memory_kwh_mb)


        # Setup Transmission matrix -> basically co2e per gb
        self._transmission_matrix = np.zeros((len(regions_indicies), len(regions_indicies)))
        for from_region_index in region_index:
            for to_region_index in region_index:
                data_transfer_co2e = data_source_manager.get_region_to_region_data("data_transfer_co2e", from_region_index, to_region_index)

                self._transmission_matrix[from_region_index][to_region_index] = data_transfer_co2e

    def get_transmission_value(self, from_instance_index: int, to_instance_index: int, from_region_index: int, to_region_index: int) -> float:
        # We simply use this to get the data transfer size and calculate co2e movement
        transmission_size = self._data_source_manager.get_instance_to_instance_data("data_transfer_size", from_instance_index, to_instance_index)
        transmission_co2e_per_gb = self._transmission_matrix[from_region_index][to_region_index]

        return self._carbon_calculator.calculate_transmission_carbon(transmission_co2e_per_gb, transmission_size)