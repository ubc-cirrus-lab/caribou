from ..loader import Loader

import numpy as np

class InstanceLoader(Loader):
    def __init__(self):
        super().__init__()
    
    def setup(self, instances: np.ndarray, carbon_information: dict, datacenter_information: dict) -> None:
        self._data = {}

        for instance in instances:
            self._data[instance] = {
                "carbon": carbon_information.get(instance, 1000),
                "datacenter": datacenter_information.get(instance, 1000)
            }
    
    def retrieve_data(self, *args, **kwargs) -> dict:
        return self._data