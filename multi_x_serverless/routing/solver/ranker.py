import numpy as np

from multi_x_serverless.routing.solver.workflow_config import WorkflowConfig


class Ranker:
    def __init__(self, config: WorkflowConfig):
        self._config = config

    def rank(self, results: np.ndarray) -> np.ndarray:
        raise NotImplementedError
