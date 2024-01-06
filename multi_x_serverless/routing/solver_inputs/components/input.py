from abc import ABC, abstractmethod

import numpy as np

class Input(ABC):
    def __init__(self):
        self._cache: dict(str, float) = {}
    
    @abstractmethod
    def setup(self, *args, **kwargs) -> None:
        # Clear Cache
        self._cache = {}

    @abstractmethod
    def get_execution_value(self, instance_index: int, region_index: int) -> float:
        raise NotImplementedError

    @abstractmethod
    def get_transmission_value(self, from_instance_index: int, to_instance_index: int, from_region_index: int, to_region_index: int) -> float:
        raise NotImplementedError
    
    def __str__(self) -> str:
        return f"SolverInput(name={self.__class__.__name__})"

    def __repr__(self) -> str:
        return self.__str__()