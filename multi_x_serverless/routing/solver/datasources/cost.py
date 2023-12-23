import numpy as np

from multi_x_serverless.routing.solver.datasources.source import Source


class CostSource(Source):
    def __init__(self, name: str, type: str, data: np.ndarray):
        super().__init__(name, type, data)

    def get_execution_matrix(self, regions: np.ndarray, functions: np.ndarray) -> np.ndarray:
        # TODO (#15): Implement this function
        return np.zeros((len(regions), len(functions)))

    def get_transmission_matrix(self, regions: np.ndarray) -> np.ndarray:
        # TODO (#15): Implement this function
        return np.zeros((len(regions), len(regions)))
