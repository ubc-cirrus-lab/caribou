import numpy as np

from multi_x_serverless.routing.current.workflow_config import WorkflowConfig


class Source:
    _data: np.ndarray

    def __init__(self, config: WorkflowConfig, regions: np.ndarray, functions: list[dict]):
        self._config = config
        self._regions = regions
        self._functions = functions

    def _set_data(self, data: np.ndarray) -> None:
        self._data = data

    def __str__(self) -> str:
        return f"Source(name={self.__class__.__name__})"

    def __repr__(self) -> str:
        return self.__str__()

    def get_execution_matrix(self) -> np.ndarray:
        raise NotImplementedError

    def get_transmission_matrix(self) -> np.ndarray:
        raise NotImplementedError
