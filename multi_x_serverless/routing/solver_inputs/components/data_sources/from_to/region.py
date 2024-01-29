from typing import Any

import numpy as np

# Indexers
from multi_x_serverless.routing.models.indexer import Indexer
from multi_x_serverless.routing.solver_inputs.components.data_sources.source import Source


class RegionToRegionSource(Source):
    def __init__(self) -> None:
        super().__init__()

    def setup(self, loaded_data: dict, regions: list[tuple[str, str]], regions_indexer: Indexer) -> None:
        self._data = {}

        # Known information
        for from_region in regions:
            from_region_index = regions_indexer.value_to_index(from_region)
            for to_region in regions:
                to_region_index = regions_indexer.value_to_index(to_region)

                if from_region_index not in self._data:
                    self._data[from_region_index] = {}

                self._data[from_region_index][to_region_index] = {
                    # Data Collector information
                    ## CO2 information
                    "data_transfer_co2e": loaded_data.get("data_transfer_co2e", {}).get((from_region, to_region), -1),
                    ## Datacenter information
                    "data_transfer_ingress_cost": loaded_data.get("data_transfer_ingress_cost", {}).get(
                        (from_region, to_region), -1
                    ),
                    "data_transfer_egress_cost": loaded_data.get("data_transfer_egress_cost", {}).get(
                        (from_region, to_region), -1
                    ),
                    "transmission_times": loaded_data.get("transmission_times", {}).get((from_region, to_region), []),
                }

    def get_value(self, data_name: str, from_region_index: int, to_region_index: int) -> Any:
        # Result type might not necessarily be float
        # For example transmission_times is a list of data transfer size to expected
        # Network latency.
        return self._data[from_region_index][to_region_index][data_name]
