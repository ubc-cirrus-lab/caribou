from ..source import Source
import numpy as np

class RegionToRegionSource(Source):
    def __init__(self):
        super().__init__()
    
    def setup(self, regions: np.ndarray, carbon_from_to_information: dict, datacenter_from_to_information: dict) -> None:
        self._data = {}

        for from_region in regions:
            from_region = (from_region[0], from_region[1])
            self._data[from_region] = {}
            for to_region in regions:
                to_region = (to_region[0], to_region[1])
                self._data[from_region][to_region] = {
                    "carbon": carbon_from_to_information.get(from_region, {}).get(to_region, 1000),
                    "datacenter": datacenter_from_to_information.get(from_region, {}).get(to_region, 1000)
                }
    
    def get_value(self, data_name: str, from_region: tuple(str, str), to_region: tuple(str, str)) -> float:
        return self._data[from_region][to_region][data_name]