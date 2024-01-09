from ..loader import Loader

import numpy as np

class CarbonRegionLoader(Loader):
    def __init__(self):
        super().__init__()
    
    def setup(self, regions: np.ndarray) -> None:
        self._data = {}

        # TODO: Load data from database, convert to proper format and store in self._data
        
        # template of output data
        self._data = {
            "grid_co2e": "PLACEHOLDER: loaded dictionary (region_name: grid_co2e in gCO2eq/kWh)",
        }

        return (False, "Not yet implemented")