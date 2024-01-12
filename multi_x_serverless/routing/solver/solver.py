from abc import ABC, abstractmethod
from typing import Optional

import numpy as np

from ..formatter.formatter import Formatter
from ..models.dag import DAG
from ..models.region import Region
from ..ranker.ranker import Ranker
from ..solver_inputs.input_manager import InputManager
from ..workflow_config import WorkflowConfig


class Solver(ABC):
    def __init__(self, workflow_config: WorkflowConfig):
        self._workflow_config = workflow_config

        # Setup the input manager (Not fully instantiated yet)
        self._input_manager = InputManager(workflow_config)

        # Get all regions allowed for the workflow
        self.worklow_level_permitted_regions = self._filter_regions_global(self._input_manager.get_all_regions())

        # Set up the instance indexer (DAG) and region indexer
        self._dag = self.get_dag_representation()
        self._region_indexer = Region(self.worklow_level_permitted_regions)

        # Setup the ranker for final ranking of solutions
        self._ranker = Ranker(workflow_config)

        # Setup the formatter for formatting final output
        self._formatter = Formatter()

        # Initiate input_manager
        self._instantiate_input_manager()

    def solve(self) -> list[tuple[dict, float, float, float]]:
        solved_results = self._solve(self.worklow_level_permitted_regions)
        ranked_results = self.rank_solved_results(solved_results)
        return self._formatter.format(ranked_results)

    @abstractmethod
    def _solve(self, regions: list[dict]) -> list[tuple[dict, float, float, float]]:
        raise NotImplementedError

    def _filter_regions(self, regions: list[dict], regions_and_providers: dict) -> list[dict]:
        # Take in a list of regions, then apply filters to remove regions that do not satisfy the constraints
        # print(regions)
        # print(regions_and_providers)
        # First filter out regions that are not in the provider list
        provider_names = [provider["name"] for provider in regions_and_providers["providers"]]
        regions = [region for region in regions if region["provider"] in provider_names]

        # Then if the user set a allowed_regions, only permit those regions and return
        if "allowed_regions" in regions_and_providers and regions_and_providers["allowed_regions"] is not None:
            return [region for region in regions if region in regions_and_providers["allowed_regions"]]

        # Finally we filter out regions that the user doesn't want to use
        if "disallowed_regions" in regions_and_providers and regions_and_providers["disallowed_regions"] is not None:
            regions = [region for region in regions if region not in regions_and_providers["disallowed_regions"]]

        return regions

    def _filter_regions_global(self, regions: list[dict]) -> list[dict]:
        return self._filter_regions(regions, self._workflow_config.regions_and_providers)

    def _filter_regions_instance(self, regions: list[dict], instance_index: str) -> list[dict]:
        return self._filter_regions(regions, self._workflow_config.instances[instance_index]["regions_and_providers"])

    def rank_solved_results(
        self, results: list[tuple[dict, float, float, float]]
    ) -> list[tuple[dict, float, float, float]]:
        return self._ranker.rank(results)

    def _instantiate_input_manager(self) -> None:
        self._input_manager.setup(self._region_indexer, self._dag)

    def get_dag_representation(self) -> DAG:
        nodes = [
            {k: v for k, v in node.items() if k != "succeeding_instances" and k != "preceding_instances"}
            for node in self._workflow_config.instances
        ]
        dag = DAG(nodes)
        for instance in self._workflow_config.instances:
            for succeeding_instance in instance["succeeding_instances"]:
                dag.add_edge(instance["instance_name"], succeeding_instance)
        return dag

    def _fail_hard_resource_constraints(
        self, constraints: Optional[dict], cost: float, runtime: float, carbon: float
    ) -> bool:
        if constraints is None or "hard_resource_constraints" not in constraints:
            return False
        hard_resource_constraints = constraints["hard_resource_constraints"]
        return (
            "cost" in hard_resource_constraints
            and cost > hard_resource_constraints["cost"]["value"]
            or "runtime" in hard_resource_constraints
            and runtime > hard_resource_constraints["runtime"]["value"]
            or "carbon" in hard_resource_constraints
            and carbon > hard_resource_constraints["carbon"]["value"]
        )
