from abc import ABC, abstractmethod

import numpy as np

from multi_x_serverless.routing.current.ranker.ranker import Ranker
from multi_x_serverless.routing.current.workflow_config import WorkflowConfig


class Solver(ABC):
    def __init__(self, workflow_config: WorkflowConfig):
        self._workflow_config = workflow_config
        self._ranker = Ranker(workflow_config)

    @abstractmethod
    def solve(self, regions: np.ndarray, functions: np.ndarray) -> list[tuple[dict, float, float, float]]:
        raise NotImplementedError

    def rank_solved_results(
        self, results: list[tuple[dict, float, float, float]]
    ) -> list[tuple[dict, float, float, float]]:
        return self._ranker.rank(results)
