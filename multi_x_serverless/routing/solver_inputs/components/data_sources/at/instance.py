from ..source import Source
import numpy as np

class InstanceSource(Source):
    def __init__(self):
        super().__init__()
    
    def setup(self, instances: np.ndarray, carbon_information: dict, datacenter_information: dict) -> None:
        self._data = {}

        for instance in instances:
            self._data[instance] = {
                "carbon": carbon_information.get(instance, 1000),
                "datacenter": datacenter_information.get(instance, 1000)
            }
    
    def get_value(self, data_name: str, instance: str) -> float:
        return self._data[instance][data_name]