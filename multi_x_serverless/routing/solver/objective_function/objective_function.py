from abc import ABC, abstractmethod
from typing import Optional


class ObjectiveFunction(ABC):
    @staticmethod
    @abstractmethod
    def calculate(cost: float, runtime: float, carbon: float, **kwargs: Optional[float]) -> bool:
        raise NotImplementedError
