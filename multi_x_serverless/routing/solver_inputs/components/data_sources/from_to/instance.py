# Source is an abstract class that is used to define the interface for all data sources.
from ..source import Source

# Indexers
from .....models.indexer import Indexer

import numpy as np

class InstanceToInstanceSource(Source):
    def __init__(self):
        super().__init__()
    
    def setup(self, loaded_data: dict, instances: list[str], instance_indexer: Indexer) -> None:
        self._data = {}

        # Known information
        for from_instance in instances:
            from_instance_index = instance_indexer.value_to_index(from_instance)
            for to_instance in instances:
                to_instance_index = instance_indexer.value_to_index(to_instance)

                if from_instance_index not in self._data:
                    self._data[from_instance_index] = {}

                self._data[from_instance_index][to_instance_index] = {
                    "data_transfer_size": loaded_data.get('data_transfer_size', {}).get((from_instance, to_instance), -1),
                    "transmission_time": loaded_data.get('transmission_time', {}).get((from_instance, to_instance), -1),
                }

    def get_value(self, data_name: str, from_instance_index: int, to_instance_index: int) -> float:
        return self._data[from_instance_index][to_instance_index][data_name]