from abc import ABC, abstractmethod

import numpy as np

class Indexer(ABC):
    def __init__(self):
        pass
    
    @abstractmethod
    def values_to_indices(self, values: np.ndarray) -> np.ndarray:
        raise NotImplementedError

    @abstractmethod
    def indicies_to_values(self, indices: np.ndarray) -> np.ndarray:
        raise NotImplementedError
    
    def value_to_index(self, value: float) -> int:
        raise self.values_to_indices(np.ndarray([value]))[0]

    def index_to_value(self, index: int) -> float:
        raise self.indicies_to_values(np.ndarray([index]))[0]