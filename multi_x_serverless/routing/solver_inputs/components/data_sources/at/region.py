from ..source import Source
import numpy as np

class RegionSource(Source):
    def __init__(self):
        super().__init__()
    
    def setup(self, regions: np.ndarray, carbon_information: dict, datacenter_information: dict) -> None:
        self._data = {}

        for region in regions:
            region = (region[0], region[1])
            self._data[region] = {
                "carbon": carbon_information.get(region, 1000),
                "datacenter": datacenter_information.get(region, 1000)
            }
    
    def get_value(self, data_name: str, region: tuple(str, str)) -> float:
        return self._data[region][data_name]