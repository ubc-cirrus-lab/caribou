from ..source import Source
import numpy as np

# Indexers
from .....models.indexer import Indexer

class InstanceToInstanceSource(Source):
    def __init__(self):
        super().__init__()
    
    def setup(self, loaded_data: dict, regions_indexer: Indexer, instance_indexer: Indexer) -> None:
        self._data = {}

        for from_instance in instances:
            self._data[from_instance] = {}
            for to_instance in instances:
                self._data[from_instance][to_instance] = {
                    "carbon": carbon_from_to_information.get(from_instance, {}).get(to_instance, 1000),
                    "datacenter": datacenter_from_to_information.get(from_instance, {}).get(to_instance, 1000)
                }

    def get_value(self, data_name: str, from_instance: str, to_instance: str) -> float:
        return self._data[from_instance][to_instance][data_name]