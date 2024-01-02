import numpy as np

from multi_x_serverless.routing.current.data_sources.source import Source


class RuntimeSource(Source):
    def __init__(self, name: str, type: str, regions: np.ndarray, functions: np.ndarray):
        data = np.zeros((len(regions), len(functions)))
        super().__init__(name, type, data, regions, functions)

    def get_execution_matrix(self) -> np.ndarray:
        # TODO (#15): Implement this function
        return np.zeros((len(self._regions), len(self._functions)))

    def get_transmission_matrix(self) -> np.ndarray:
        # TODO (#15): Implement this function
        return np.zeros((len(self._regions), len(self._regions)))
