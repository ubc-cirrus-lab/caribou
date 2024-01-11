from ..loader import Loader

import numpy as np

class DataCenterRegionToRegionLoader(Loader):
    def __init__(self):
        super().__init__()
    
    def setup(self, regions: list[(str, str)]) -> bool:
        self._data = {}
        
        # TODO: Load data from database, convert to proper format and store in self._data

        # template of output data
        self._data = {
            "data_transfer_ingress_cost": {}, # "PLACEHOLDER: loaded dictionary (((from_region_provider, from_region_name), (to_region_provider, to_region_name)): cost in USD)",
            "data_transfer_egress_cost": {}, # "PLACEHOLDER: loaded dictionary (((from_region_provider, from_region_name), (to_region_provider, to_region_name)): cost in USD)",
            "transmission_times": {}, # "PLACEHOLDER: loaded dictionary ((from_instance_name, to_instance_name): [(size of package, transmission time in seconds)])"
        }

        return False # Not implemented