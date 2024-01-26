from abc import ABC, abstractmethod
from typing import Any

class DataRetriever(ABC):
    @abstractmethod
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError