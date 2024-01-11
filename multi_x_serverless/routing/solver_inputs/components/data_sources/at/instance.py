# Source is an abstract class that is used to define the interface for all data sources.
from ..source import Source

# Indexers
from .....models.indexer import Indexer

import numpy as np

class InstanceSource(Source):
    def __init__(self):
        super().__init__()
    
    def setup(self, loaded_data: dict, instances: list[str], instance_indexer: Indexer) -> None:
        self._data = {}
        
        # Known information
        for instance in instances:
            instance_index = instance_indexer.value_to_index(instance)
            self._data[instance_index] = {
                "execution_time": loaded_data.get('execution_time', {}).get(instance, -1),
            }

    def get_value(self, data_name: str, instance_index: int) -> float:
        return self._data[instance_index][data_name]