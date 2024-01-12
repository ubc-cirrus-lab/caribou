from abc import ABC, abstractmethod

import numpy as np


class Input(ABC):
    def __init__(self):
        self._cache: dict(str, float) = {}

        self._data_source_manager = None

        self._execution_matrix: np.ndarray = None
        self._transmission_matrix: np.ndarray = None

    @abstractmethod
    def setup(self, *args, **kwargs) -> None:
        # Clear Cache
        self._cache = {}

        self._transmission_matrix = None
        self._execution_matrix = None

    @abstractmethod
    def get_transmission_value(
        self, from_instance_index: int, to_instance_index: int, from_region_index: int, to_region_index: int
    ) -> float:
        raise NotImplementedError

    def get_execution_value(self, instance_index: int, region_index: int) -> float:
        if self._execution_matrix is not None:
            return self._execution_matrix[region_index][instance_index]
        else:
            raise Exception(
                "Runtime matrix is not initialized. Please call setup() before calling get_execution_value()."
            )

    def __str__(self) -> str:
        return f"SolverInput(name={self.__class__.__name__})"

    def __repr__(self) -> str:
        return self.__str__()
