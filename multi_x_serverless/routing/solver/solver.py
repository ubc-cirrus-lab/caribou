from abc import ABC, abstractmethod

import numpy as np

from multi_x_serverless.routing.solver.ranker import Ranker
from multi_x_serverless.routing.solver.workflow_config import WorkflowConfig


class Solver(ABC):
    def __init__(self, workflow_config: WorkflowConfig):
        self._workflow_config = workflow_config
        self._ranker = Ranker(workflow_config)

    @abstractmethod
    def solve(self, regions: np.ndarray, functions: np.ndarray) -> np.ndarray:
        raise NotImplementedError

    def rank_solved_results(self, results: np.ndarray) -> np.ndarray:
        return self._ranker.rank(results)
