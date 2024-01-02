from abc import ABC, abstractmethod

import numpy as np

from multi_x_serverless.routing.current.ranker.ranker import Ranker
from multi_x_serverless.routing.current.workflow_config import WorkflowConfig
from multi_x_serverless.routing.current.data_sources.source import Source
from multi_x_serverless.routing.current.data_sources.carbon import CarbonSource
from multi_x_serverless.routing.current.data_sources.cost import CostSource
from multi_x_serverless.routing.current.data_sources.runtime import RuntimeSource


class Solver(ABC):
    __data_sources: dict[str, Source] = {
        "carbon": CarbonSource,
        "cost": CostSource,
        "runtime": RuntimeSource,
    }

    def __init__(self, workflow_config: WorkflowConfig):
        self._workflow_config = workflow_config
        self._ranker = Ranker(workflow_config)

    def solve(self, regions: np.ndarray) -> list[tuple[dict, float, float, float]]:
        filtered_regions = self._instantiate_data_sources(self._filter_regions(regions))
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

    def _instantiate_data_sources(self, regions: np.ndarray):
        for name, source in self.__data_sources.items():
            self.__data_sources[name] = source(self._workflow_config, regions, self._workflow_config.functions)
