from ..loader import Loader

import numpy as np

class DataCenterRegionLoader(Loader):
    def __init__(self):
        super().__init__()
    
    def setup(self, regions: list[(str, str)]) -> bool:
        self._data = {}

        # TODO: Load data from database, convert to proper format and store in self._data

        # template of output data
        self._data = {
            "compute_cost": {}, # "PLACEHOLDER: loaded dictionary ((region_provider, region_name): *****Compute cost list(this need to be treated differently as providers scale cost base on calls))",
            "pue": {}, # "PLACEHOLDER: loaded dictionary ((region_provider, region_name): PUE)",
            "cfe": {}, # "PLACEHOLDER: loaded dictionary ((region_provider, region_name): CFE)",
            "compute_kwh": {}, # "PLACEHOLDER: loaded dictionary ((region_provider, region_name): compute kwh)",
            "memory_kwh_mb": {}, # "PLACEHOLDER: loaded dictionary ((region_provider, region_name): memory kwh/MB)",
            "Free_tier": {}, # "PLACEHOLDER: loaded dictionary ((region_provider, region_name): free tier informations (Implement at the same time as we implement for free tier issue #27))",
        }

        return False # Not implemented