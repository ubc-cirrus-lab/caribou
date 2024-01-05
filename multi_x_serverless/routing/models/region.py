from multi_x_serverless.routing.models.indexer import Indexer

import numpy as np

from multi_x_serverless.routing.workflow_config import WorkflowConfig


class Region(Indexer):
    # TODO (#15): Implement this class
    def __init__(self, workflow_config: WorkflowConfig) -> None:
        self._workflow_config = workflow_config
        # Need to implement indecies
        self._region_indices: dict[str, int] = None
        # self._region_indices = dict[str, int] = {node["instance_name"]: index for index, node in enumerate(nodes)}

    def get_all_regions(self) -> np.ndarray:
        # TODO (#15): Implement this function
        return np.array([])

    def values_to_indices(self, regions: np.ndarray) -> np.ndarray:
        return np.array([self._region_indices[(region[0], region[1])] for region in regions])
    
    def indicies_to_values(self, indices: np.ndarray) -> np.ndarray:
        # Can be optimized
        reverse_mapping = {index: region for region, index in self._region_indices.items()}
        return np.array([reverse_mapping.get(index) for index in indices])