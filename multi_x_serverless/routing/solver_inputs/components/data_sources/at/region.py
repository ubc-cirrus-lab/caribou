# Source is an abstract class that is used to define the interface for all data sources.
import numpy as np

# Indexers
from multi_x_serverless.routing.models.indexer import Indexer
from multi_x_serverless.routing.solver_inputs.components.data_sources.source import Source

class RegionSource(Source):
    def __init__(self):
        super().__init__()

    def setup(self, loaded_data: dict, regions: list[(str, str)], regions_indexer: Indexer) -> None:
        self._data = {}

        # Known information
        for region in regions:
            region_index = regions_indexer.value_to_index(region)
            self._data[region_index] = {
                # Region location
                "provider_name": region[0],  # Save the provider name
                # CO2 information
                "grid_co2e": loaded_data.get("grid_co2e", {}).get(region, -1),
                # Datacenter information
                "compute_costs": loaded_data.get("compute_costs", {}).get(region, []),
                "pue": loaded_data.get("pue", {}).get(region, -1),
                "cfe": loaded_data.get("cfe", {}).get(region, -1),
                "average_kw_compute": loaded_data.get("average_kw_compute", {}).get(region, -1),
                "memory_kw_mb": loaded_data.get("memory_kw_mb", {}).get(region, -1),
                "free_tier": loaded_data.get("free_tier", {}).get(region, -1),
            }

    def get_value(self, data_name: str, region_index: int):  # Result type might not necessarily be float
        return self._data[region_index][data_name]
