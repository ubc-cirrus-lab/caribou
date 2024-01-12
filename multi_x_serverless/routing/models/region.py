import numpy as np

from multi_x_serverless.routing.models.indexer import Indexer
from multi_x_serverless.routing.workflow_config import WorkflowConfig


class Region(Indexer):
    # TODO (#15): Implement this class
    def __init__(self, regions: list[dict[str, str]]) -> None:
        # This is imply _region_indicies
        self._value_indices: dict[(str, str), int] = {
            (region["provider"], region["region"]): index for index, region in enumerate(regions)
        }

    def values_to_indices(self, regions: np.ndarray) -> np.ndarray:
        return np.array([self._value_indices[(region[0], region[1])] for region in regions])

    def indicies_to_values(self, indices: np.ndarray) -> np.ndarray:
        # Can be optimized
        reverse_mapping = {index: region for region, index in self._value_indices.items()}
        return np.array([reverse_mapping.get(index) for index in indices])
