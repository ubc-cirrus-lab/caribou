import typing
from abc import ABC, abstractmethod

import numpy as np


class Indexer(ABC):
    _value_indices: dict

    def get_value_indices(self) -> dict:
        return self._value_indices

    @abstractmethod
    def values_to_indices(self, values: np.ndarray) -> np.ndarray:
        raise NotImplementedError

    @abstractmethod
    def indicies_to_values(self, indices: np.ndarray) -> np.ndarray:
        raise NotImplementedError

    def value_to_index(self, value: typing.Any) -> int:
        return int(self.values_to_indices(np.array([value]))[0])

    def index_to_value(self, index: int) -> typing.Any:
        return self.indicies_to_values(np.array([index]))[0]
