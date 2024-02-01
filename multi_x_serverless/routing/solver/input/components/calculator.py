from abc import ABC, abstractmethod
from typing import Any, Callable


class InputCalculator(ABC):
    _data_cache: dict[str, Any]

    def setup(self, *args: Any, **kwargs: Any) -> None:
        self._data_cache = {}

    def __str__(self) -> str:
        return f"InputCalculator(name={self.__class__.__name__})"

    def __repr__(self) -> str:
        return self.__str__()
