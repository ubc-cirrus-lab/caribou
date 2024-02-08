from abc import ABC, abstractmethod

import numpy as np

from multi_x_serverless.routing.solver_inputs.components.data_sources.data_source_manager import DataSourceManager


class Input(ABC):
    _data_source_manager: DataSourceManager
    _execution_matrix: np.ndarray

    @abstractmethod
    def get_transmission_value(
        self,
        from_instance_index: int,
        to_instance_index: int,
        from_region_index: int,
        to_region_index: int,
        consider_probabilistic_invocations: bool,
    ) -> float:
        raise NotImplementedError

    def get_execution_value(
        self, instance_index: int, region_index: int, consider_probabilistic_invocations: bool
    ) -> float:
        # TODO (#35): Implement probabilistic invocations
        print(consider_probabilistic_invocations)
        if self._execution_matrix is not None:
            return float(self._execution_matrix[region_index][instance_index])
        raise RuntimeError(
            "Runtime matrix is not initialized. Please call setup() before calling get_execution_value()."
        )

    def __str__(self) -> str:
        return f"SolverInput(name={self.__class__.__name__})"

    def __repr__(self) -> str:
        return self.__str__()
