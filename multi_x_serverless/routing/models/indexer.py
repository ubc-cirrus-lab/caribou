from abc import ABC, abstractmethod

import numpy as np


class Indexer(ABC):
    def __init__(self):
        self._value_indices = None
        pass

    def get_value_indices(self) -> dict:
        return self._value_indices

    @abstractmethod
    def values_to_indices(self, values: np.ndarray) -> np.ndarray:
        raise NotImplementedError

    @abstractmethod
    def indicies_to_values(self, indices: np.ndarray) -> np.ndarray:
        raise NotImplementedError

    def value_to_index(self, value) -> int:
        return self.values_to_indices(np.array([value]))[0]

    def index_to_value(self, index: int):
        return self.indicies_to_values(np.array([index]))[0]
