from typing import Optional

from multi_x_serverless.routing.models.indexer import Indexer
from multi_x_serverless.routing.solver_inputs.components.data_sources.at.at_source import AtSource


class RegionSource(AtSource):
    def setup(
        self,
        loaded_data: dict,
        items_to_source: list,
        indexer: Indexer,
        _: Optional[list[dict]] = None,
    ) -> None:
        self._data = {}

        # Known information
        for region in items_to_source:
            region_index = indexer.value_to_index(region)
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
