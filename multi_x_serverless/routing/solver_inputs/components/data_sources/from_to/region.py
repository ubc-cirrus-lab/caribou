# Source is an abstract class that is used to define the interface for all data sources.
from ..source import Source

# Indexers
from .....models.indexer import Indexer

import numpy as np

class RegionToRegionSource(Source):
    def __init__(self):
        super().__init__()
    
    def setup(self, loaded_data: dict, regions: list[(str, str)], regions_indexer: Indexer) -> bool:
        self._data = {}

        # Known information
        for from_region in regions:
            from_region_index = regions_indexer.value_to_index(from_region)
            for to_region in regions:
                to_region_index = regions_indexer.value_to_index(to_region)

                if from_region_index not in self._data:
                    self._data[from_region_index] = {}

                self._data[from_region_index][to_region_index] = {
                    # CO2 information
                    "data_transfer_co2e": loaded_data.get('data_transfer_co2e', {}).get((from_region, to_region), -1),

                    # Datacenter information
                    "data_transfer_price": loaded_data.get('data_transfer_price', {}).get((from_region, to_region), -1),
                }

        return True
    
    def get_value(self, data_name: str, from_region: tuple(str, str), to_region: tuple(str, str)) -> float:
        return self._data[from_region][to_region][data_name]