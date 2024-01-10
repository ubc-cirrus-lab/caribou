from ..loader import Loader

import numpy as np

class DataCenterRegionToRegionLoader(Loader):
    def __init__(self):
        super().__init__()
    
    def setup(self, regions: np.ndarray) -> bool:
        self._data = {}
        
        # TODO: Load data from database, convert to proper format and store in self._data

        # template of output data
        self._data = {
            "data_transfer_price": {}, # "PLACEHOLDER: loaded dictionary ((from_region_name, to_region_name): cost in USD)",
        }

        return False # Not implemented