import numpy as np

from multi_x_serverless.routing.current.workflow_config import WorkflowConfig


class Ranker:
    def __init__(self, config: WorkflowConfig):
        self._config = config

    def rank(self, results: list[tuple[dict, float, float, float]]) -> list[tuple[dict, float, float, float]]:
        # TODO (#23): Implement ranker
        raise NotImplementedError
