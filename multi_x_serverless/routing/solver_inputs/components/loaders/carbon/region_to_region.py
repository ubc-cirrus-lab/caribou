from ..loader import Loader

import numpy as np

class CarbonRegionFromToLoader(Loader):
    def __init__(self):
        super().__init__()
    
    def setup(self, regions: np.ndarray) -> None:
        self._data = {}

        # TODO: Load data from database, convert to proper format and store in self._data

        # template of output data
        self._data = {
            "data_transfer_co2e": "PLACEHOLDER: loaded dictionary ((from_region_name, to_region_name): gCO2eq/GB)",
        }

        return (False, "Not yet implemented")