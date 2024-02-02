from abc import ABC
from typing import Any


class Source(ABC):
    def __init__(self) -> None:
        self._data: dict[Any, Any] = {}
