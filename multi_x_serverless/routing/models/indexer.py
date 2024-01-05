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