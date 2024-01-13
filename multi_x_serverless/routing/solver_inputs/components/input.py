from abc import ABC, abstractmethod

import numpy as np

from multi_x_serverless.routing.solver_inputs.components.data_sources.data_source_manager import DataSourceManager


class Input(ABC):
    _data_source_manager: DataSourceManager
    _execution_matrix: np.ndarray
    _transmission_matrix: np.ndarray

    def __init__(self) -> None:
        self._cache: dict[str, float] = {}

    @abstractmethod
    def get_transmission_value(
        self, from_instance_index: int, to_instance_index: int, from_region_index: int, to_region_index: int
    ) -> float:
        raise NotImplementedError

    def get_execution_value(self, instance_index: int, region_index: int) -> float:
        if self._execution_matrix is not None:
            return float(self._execution_matrix[region_index][instance_index])
        else:
            raise Exception(
                "Runtime matrix is not initialized. Please call setup() before calling get_execution_value()."
            )

    def __str__(self) -> str:
        return f"SolverInput(name={self.__class__.__name__})"

    def __repr__(self) -> str:
        return self.__str__()
