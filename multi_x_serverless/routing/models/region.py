import numpy as np

from multi_x_serverless.routing.models.indexer import Indexer
from multi_x_serverless.routing.workflow_config import WorkflowConfig


class Region(Indexer):
    def __init__(self, regions: list[dict[str, str]]) -> None:
        self._value_indices: dict[tuple[str, str], int] = {
            (region["provider"], region["region"]): index for index, region in enumerate(regions)
        }

    def values_to_indices(self, regions: np.ndarray) -> np.ndarray:
        return np.array([self._value_indices[(region[0], region[1])] for region in regions])

    def indicies_to_values(self, indices: np.ndarray) -> np.ndarray:
        # Can be optimized - Can be optimized by only performing reverse mapping list creation once, 
        # and simply use this already created list. Trouble is with keeping it consistent 
        # (if the self._value_indices changed, this should also change for example). 
        # Should be optimized as it or the variation of value_to_index and index_to_value may be called many times.
        reverse_mapping = {index: region for region, index in self._value_indices.items()}
        return np.array([reverse_mapping.get(index) for index in indices])
