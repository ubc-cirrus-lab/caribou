import typing

import numpy as np

# Indexers
from multi_x_serverless.routing.models.indexer import Indexer
from multi_x_serverless.routing.solver_inputs.components.data_sources.source import Source


class RegionSource(Source):
    def __init__(self) -> None:
        super().__init__()

    def setup(self, loaded_data: dict, regions: list[tuple[str, str]], regions_indexer: Indexer) -> None:
        self._data = {}

        # Known information
        for region in regions:
            region_index = regions_indexer.value_to_index(region)
            self._data[region_index] = {
                # Other properties
                ## Region location - From region properties
                "provider_name": region[0],  # Save the provider name
                # Data Collector information
                ## CO2 information
                "grid_co2e": loaded_data.get("grid_co2e", {}).get(region, -1),
                ## Datacenter information
                "compute_cost": loaded_data.get("compute_cost", {}).get(region, -1),
                "pue": loaded_data.get("pue", {}).get(region, -1),
                "cfe": loaded_data.get("cfe", {}).get(region, -1),
                "average_kw_compute": loaded_data.get("average_kw_compute", {}).get(region, -1),
                "memory_kw_mb": loaded_data.get("memory_kw_mb", {}).get(region, -1),
                "free_tier_invocations": loaded_data.get("free_tier_invocations", {}).get(region, -1),
                "free_tier_compute": loaded_data.get("free_tier_compute", {}).get(region, -1),
            }

    def get_value(self, data_name: str, region_index: int) -> typing.Any:
        # Result type might not necessarily be float
        # For example provider_name is a string
        return self._data[region_index][data_name]
