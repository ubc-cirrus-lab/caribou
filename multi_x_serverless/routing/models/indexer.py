from abc import ABC, abstractmethod
from typing import Any

import numpy as np


class Indexer(ABC):
    def __init__(self) -> None:
        if not hasattr(self, "_value_indices"):
            self._value_indices: dict[Any, int] = {}

        self._indices_to_values: dict[int, Any] = self.indicies_to_values()

    def get_value_indices(self) -> dict[Any, int]:
        return self._value_indices

    def indicies_to_values(self) -> dict[int, Any]:
        return {index: instance for instance, index in self._value_indices.items()}

    def value_to_index(self, value: Any) -> int:
        return self._value_indices[value]

    def index_to_value(self, index: int) -> Any:
        return self._indices_to_values[index]
