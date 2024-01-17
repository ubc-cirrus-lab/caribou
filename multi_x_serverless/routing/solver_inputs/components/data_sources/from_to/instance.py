import typing

import numpy as np

# Indexers
from multi_x_serverless.routing.models.indexer import Indexer
from multi_x_serverless.routing.solver_inputs.components.data_sources.source import Source


class InstanceToInstanceSource(Source):
    def __init__(self) -> None:
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
                    # Data Collector information
                    "data_transfer_size": loaded_data.get("data_transfer_size", {}).get(
                        (from_instance, to_instance), -1
                    ),
                }

    def get_value(self, data_name: str, from_instance_index: int, to_instance_index: int) -> float:
        return self._data[from_instance_index][to_instance_index][data_name]
