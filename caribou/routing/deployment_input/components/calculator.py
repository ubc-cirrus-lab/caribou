from abc import ABC
from typing import Any


class InputCalculator(ABC):
    _data_cache: dict[str, Any]

    def setup(self) -> None:
        self._data_cache = {}

    def __str__(self) -> str:
        return f"InputCalculator(name={self.__class__.__name__})"

    def __repr__(self) -> str:
        return self.__str__()
