import json
from abc import ABC, abstractmethod
from typing import Optional

import numpy as np

from multi_x_serverless.common.constants import WORKFLOW_PLACEMENT_SOLVER_STAGING_AREA_TABLE
from multi_x_serverless.common.models.endpoints import Endpoints
from multi_x_serverless.routing.distribution_solver.input.distribution_input_manager import DistributionInputManager
from multi_x_serverless.routing.formatter.formatter import Formatter
from multi_x_serverless.routing.models.dag import DAG
from multi_x_serverless.routing.models.region import Region
from multi_x_serverless.routing.ranker.ranker import Ranker
from multi_x_serverless.routing.solver.objective_function.any_improvement_objective_function import (
    AnyImprovementObjectiveFunction,
)
from multi_x_serverless.routing.workflow_config import WorkflowConfig


class DistributionSolver(ABC):  # pylint: disable=too-many-instance-attributes
    def __init__(
        self,
        workflow_config: WorkflowConfig,
        all_available_regions: Optional[list[str]] = None,
        input_manager: Optional[DistributionInputManager] = None,
    ) -> None:
        self._workflow_config = workflow_config

        # Declare the input manager
        self._input_manager = DistributionInputManager(workflow_config, all_available_regions is None)

        # Get all regions allowed for the workflow
        if all_available_regions is None:
            all_available_regions = self._input_manager.get_all_regions()

        self._worklow_level_permitted_regions = self._filter_regions_global(all_available_regions)

        # Set up the instance indexer (DAG) and region indexer
        self._dag = self.get_dag_representation()
        self._region_indexer = Region(self._worklow_level_permitted_regions)

        if input_manager is None:
            self._instantiate_input_manager()
        else:
            self._input_manager = input_manager

        # Setup the ranker for final ranking of solutions
        self._ranker = Ranker(workflow_config)

        # Setup the formatter for formatting final output
        self._formatter = Formatter()

        self._endpoints = Endpoints()

        self._permitted_region_indices_cache: dict[int, list[int]] = {}

        self._home_region_index = self._region_indexer.value_to_index(self._workflow_config.start_hops)
        self._topological_order = self._dag.topological_sort()
        if len(self._topological_order) == 0:
            raise ValueError("DAG is empty")

        adjacency_matrix = self._dag.get_adj_matrix()
        self._adjacency_indexes = np.where(adjacency_matrix == 1)

        self._first_instance_index = self._topological_order[0]

        self._objective_function = AnyImprovementObjectiveFunction

    def solve(self) -> None:
        solved_results = self._solve(self._worklow_level_permitted_regions)
        ranked_results = self.rank_solved_results(solved_results)
        selected_result = self._select_result(ranked_results)
        formatted_result = self._formatter.format(
            selected_result, self._dag.indicies_to_values(), self._region_indexer.indicies_to_values()
        )
        self._upload_result(formatted_result)

    @abstractmethod
    def _solve(self, regions: list[str]) -> list[tuple[dict, float, float, float]]:
        raise NotImplementedError

    def _filter_regions(self, regions: list[str], regions_and_providers: dict) -> list[str]:
        # Take in a list of regions, then apply filters to remove regions that do not satisfy the constraints
        # First filter out regions that are not in the provider list
        provider_names = list(regions_and_providers["providers"].keys())
        regions = [region for region in regions if region.split(":")[0] in provider_names]

        # Then if the user set a allowed_regions, only permit those regions and return
        if "allowed_regions" in regions_and_providers and regions_and_providers["allowed_regions"] is not None:
            return [region for region in regions if region in regions_and_providers["allowed_regions"]]

        # Finally we filter out regions that the user doesn't want to use
        if "disallowed_regions" in regions_and_providers and regions_and_providers["disallowed_regions"] is not None:
            regions = [region for region in regions if region not in regions_and_providers["disallowed_regions"]]

        return regions

    def _filter_regions_global(self, regions: list[str]) -> list[str]:
        return self._filter_regions(regions, self._workflow_config.regions_and_providers)

    def _filter_regions_instance(self, regions: list[str], instance_index: int) -> list[str]:
        return self._filter_regions(regions, self._workflow_config.instances[instance_index]["regions_and_providers"])

    def rank_solved_results(
        self, results: list[tuple[dict, float, float, float]]
    ) -> list[tuple[dict, float, float, float]]:
        return self._ranker.rank(results)

    def _select_result(self, results: list[tuple[dict, float, float, float]]) -> tuple[dict, float, float, float]:
        # TODO (#48): Implement more dynamic selection of result
        return results[0]

    def _instantiate_input_manager(self) -> None:
        self._input_manager.setup(self._region_indexer, self._dag)

    def _upload_result(
        self,
        result: dict,
    ) -> None:
        result_json = json.dumps(result)
        self._endpoints.get_solver_workflow_placement_decision_client().set_value_in_table(
            WORKFLOW_PLACEMENT_SOLVER_STAGING_AREA_TABLE, self._workflow_config.workflow_id, result_json
        )

    def get_dag_representation(self) -> DAG:
        nodes = [
            {k: v for k, v in node.items() if k not in ("succeeding_instances", "preceding_instances")}
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

    def _get_permitted_region_indices(self, regions: list[str], instance: int) -> list[int]:
        if instance in self._permitted_region_indices_cache:
            return self._permitted_region_indices_cache[instance]

        permitted_regions: list[str] = self._filter_regions_instance(regions, instance)
        if len(permitted_regions) == 0:  # Should never happen in a valid DAG
            raise ValueError("There are no permitted regions for this instance")

        all_regions_indices = self._region_indexer.get_value_indices()
        permitted_regions_indices = [all_regions_indices[region] for region in permitted_regions]
        self._permitted_region_indices_cache[instance] = permitted_regions_indices
        return permitted_regions_indices
