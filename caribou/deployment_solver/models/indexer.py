from abc import ABC
from typing import Any


class Indexer(ABC):
    def __init__(self) -> None:
        if not hasattr(self, "_value_indices"):
            self._value_indices: dict[str, int] = {}

        self._indices_to_values: dict[int, str] = self.indicies_to_values()

    def get_value_indices(self) -> dict[str, int]:
        return self._value_indices

    def indicies_to_values(self) -> dict[int, str]:
        return {index: instance for instance, index in self._value_indices.items()}

    def value_to_index(self, value: str) -> int:
        return self._value_indices[value]

    def index_to_value(self, index: int) -> str:
        return self._indices_to_values[index]

    def to_dict(self) -> dict[str, Any]:
        return {"value_indices": self._value_indices, "indices_to_values": self._indices_to_values}
