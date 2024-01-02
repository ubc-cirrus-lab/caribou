import numpy as np

from multi_x_serverless.routing.current.data_sources.source import Source

from multi_x_serverless.routing.current.workflow_config import WorkflowConfig


class CarbonSource(Source):
    def __init__(self, config: WorkflowConfig, regions: np.ndarray, functions: np.ndarray):
        data = np.zeros((len(regions), len(functions)))
        self._set_data(data)

    def get_execution_matrix(self) -> np.ndarray:
        # TODO (#15): Implement this function
        return np.zeros((len(self._regions), len(self._functions)))

    def get_transmission_matrix(self) -> np.ndarray:
        # TODO (#15): Implement this function
        return np.zeros((len(self._regions), len(self._regions)))
