from abc import ABC, abstractmethod

import numpy as np

from multi_x_serverless.routing.current.data_sources.carbon import CarbonSource
from multi_x_serverless.routing.current.data_sources.cost import CostSource
from multi_x_serverless.routing.current.data_sources.runtime import RuntimeSource
from multi_x_serverless.routing.current.data_sources.source import Source
from multi_x_serverless.routing.current.ranker.ranker import Ranker
from multi_x_serverless.routing.current.workflow_config import WorkflowConfig


class Solver(ABC):
    _data_sources: dict[str, Source]

    def __init__(self, workflow_config: WorkflowConfig):
        self._workflow_config = workflow_config
        self._ranker = Ranker(workflow_config)

    def solve(self, regions: np.ndarray) -> list[tuple[dict, float, float, float]]:
        filtered_regions = self._filter_regions(regions)
        self._instantiate_data_sources(filtered_regions)
        return self._solve(filtered_regions)

    @abstractmethod
    def _solve(self, regions: np.ndarray) -> list[tuple[dict, float, float, float]]:
        raise NotImplementedError

    def _filter_regions(self, regions: np.ndarray) -> np.ndarray:
        # TODO (#21): Implement this function
        return regions

    def rank_solved_results(
        self, results: list[tuple[dict, float, float, float]]
    ) -> list[tuple[dict, float, float, float]]:
        return self._ranker.rank(results)

    def _instantiate_data_sources(self, regions: np.ndarray) -> None:
        self.__data_sources = {
            "carbon": CarbonSource(self._workflow_config, regions, self._workflow_config.functions),
            "cost": CostSource(self._workflow_config, regions, self._workflow_config.functions),
            "runtime": RuntimeSource(self._workflow_config, regions, self._workflow_config.functions),
        }
