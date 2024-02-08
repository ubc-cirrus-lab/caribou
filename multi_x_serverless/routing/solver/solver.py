import json
from abc import ABC, abstractmethod
from typing import Optional

import numpy as np

from multi_x_serverless.common.constants import WORKFLOW_PLACEMENT_SOLVER_STAGING_AREA_TABLE
from multi_x_serverless.common.models.endpoints import Endpoints
from multi_x_serverless.routing.formatter.formatter import Formatter
from multi_x_serverless.routing.models.dag import DAG
from multi_x_serverless.routing.models.region import Region
from multi_x_serverless.routing.ranker.ranker import Ranker
from multi_x_serverless.routing.solver.objective_function.any_improvement_objective_function import (
    AnyImprovementObjectiveFunction,
)
from multi_x_serverless.routing.solver.objective_function.objective_function import ObjectiveFunction
from multi_x_serverless.routing.solver_inputs.input_manager import InputManager
from multi_x_serverless.routing.workflow_config import WorkflowConfig


class Solver(ABC):  # pylint: disable=too-many-instance-attributes
    def __init__(
        self,
        workflow_config: WorkflowConfig,
        all_available_regions: Optional[list[dict]] = None,
        input_manager: Optional[InputManager] = None,
        init_home_region_transmission_costs: bool = True,
    ) -> None:
        self._workflow_config = workflow_config

        # Declare the input manager
        self._input_manager = InputManager(workflow_config)

        # Get all regions allowed for the workflow
        if not all_available_regions:
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

        self._home_region_index = self._region_indexer.value_to_index(
            self._provider_region_dict_to_tuple(self._workflow_config.start_hops)
        )
        self._topological_order = self._dag.topological_sort()
        if len(self._topological_order) == 0:
            raise ValueError("DAG is empty")

        adjacency_matrix = self._dag.get_adj_matrix()
        self._adjacency_indexes = np.where(adjacency_matrix == 1)

        self._first_instance_index = self._topological_order[0]

        if init_home_region_transmission_costs:
            self._init_home_region_transmission_costs(self._worklow_level_permitted_regions)

        self._objective_function = AnyImprovementObjectiveFunction

    def _provider_region_dict_to_tuple(self, provider_region_dict: dict[str, str]) -> tuple[str, str]:
        return (provider_region_dict["provider"], provider_region_dict["region"])

    def solve(self) -> None:
        solved_results = self._solve(self._worklow_level_permitted_regions)
        ranked_results = self.rank_solved_results(solved_results)
        selected_result = self._select_result(ranked_results)
        formatted_result = self._formatter.format(
            selected_result, self._dag.indicies_to_values(), self._region_indexer.indicies_to_values()
        )
        self._upload_result(formatted_result)

    def _init_home_region_transmission_costs(self, regions: list[dict]) -> None:
        home_region_transmissions_average = np.zeros((3, len(self._region_indexer.get_value_indices())))
        home_region_transmissions_tail = np.zeros((3, len(self._region_indexer.get_value_indices())))

        valid_region_indices_for_start_hop = self._get_permitted_region_indices(regions, self._first_instance_index)

        for region_index in valid_region_indices_for_start_hop:
            (
                home_region_transmission_costs,
                home_region_transmission_carbon,
                home_region_transmission_runtime,
            ) = self._input_manager.get_transmission_cost_carbon_runtime(
                self._first_instance_index,
                self._first_instance_index,
                self._home_region_index,
                region_index,
                consider_probabilistic_invocations=True,
            )
            home_region_transmissions_average[0, region_index] = home_region_transmission_costs
            home_region_transmissions_average[1, region_index] = home_region_transmission_runtime
            home_region_transmissions_average[2, region_index] = home_region_transmission_carbon

            (
                home_region_transmission_costs,
                home_region_transmission_carbon,
                home_region_transmission_runtime,
            ) = self._input_manager.get_transmission_cost_carbon_runtime(
                self._first_instance_index, self._first_instance_index, self._home_region_index, region_index
            )

            home_region_transmissions_tail[0, region_index] = home_region_transmission_costs
            home_region_transmissions_tail[1, region_index] = home_region_transmission_runtime
            home_region_transmissions_tail[2, region_index] = home_region_transmission_carbon

        self._home_region_transmission_costs_average = home_region_transmissions_average
        self._home_region_transmission_costs_tail = home_region_transmissions_tail

    @abstractmethod
    def _solve(self, regions: list[dict]) -> list[tuple[dict, float, float, float]]:
        raise NotImplementedError

    def _filter_regions(self, regions: list[dict], regions_and_providers: dict) -> list[dict]:
        # Take in a list of regions, then apply filters to remove regions that do not satisfy the constraints
        # First filter out regions that are not in the provider list
        provider_names = list(regions_and_providers["providers"].keys())
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

    def _filter_regions_instance(self, regions: list[dict], instance_index: int) -> list[dict]:
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

    def init_deployment_to_region(
        self, region_index: int
    ) -> tuple[
        dict[int, int],
        tuple[float, float],
        tuple[float, float],
        tuple[float, float],
        tuple[np.ndarray, np.ndarray],
        tuple[np.ndarray, np.ndarray],
    ]:
        deployment: dict[int, int] = {}
        average_node_weights = np.empty((3, len(self._topological_order)))
        tail_node_weights = np.empty((3, len(self._topological_order)))

        average_edge_weights = np.zeros((3, len(self._topological_order), len(self._topological_order)))
        tail_edge_weights = np.zeros((3, len(self._topological_order), len(self._topological_order)))

        for instance in self._workflow_config.instances:
            instance_index = self._dag.value_to_index(instance["instance_name"])
            deployment[instance_index] = region_index

            (
                tail_execution_cost,
                tail_execution_carbon,
                tail_execution_runtime,
            ) = self._input_manager.get_execution_cost_carbon_runtime(instance_index, region_index)

            tail_node_weights[0, instance_index] = tail_execution_cost
            tail_node_weights[1, instance_index] = tail_execution_runtime
            tail_node_weights[2, instance_index] = tail_execution_carbon

            (
                average_execution_cost,
                average_execution_carbon,
                average_execution_runtime,
            ) = self._input_manager.get_execution_cost_carbon_runtime(
                instance_index, region_index, consider_probabilistic_invocations=True
            )

            average_node_weights[0, instance_index] = average_execution_cost
            average_node_weights[1, instance_index] = average_execution_runtime
            average_node_weights[2, instance_index] = average_execution_carbon

        for i, j in zip(self._adjacency_indexes[0], self._adjacency_indexes[1]):
            (
                tail_transmission_cost,
                tail_transmission_carbon,
                tail_transmission_runtime,
            ) = self._input_manager.get_transmission_cost_carbon_runtime(i, j, region_index, region_index)

            tail_edge_weights[0, i, j] = tail_transmission_cost
            tail_edge_weights[1, i, j] = tail_transmission_runtime
            tail_edge_weights[2, i, j] = tail_transmission_carbon

            (
                average_transmission_cost,
                average_transmission_carbon,
                average_transmission_runtime,
            ) = self._input_manager.get_transmission_cost_carbon_runtime(
                i, j, region_index, region_index, consider_probabilistic_invocations=True
            )

            average_edge_weights[0, i, j] = average_transmission_cost
            average_edge_weights[1, i, j] = average_transmission_runtime
            average_edge_weights[2, i, j] = average_transmission_carbon

        (
            (average_cost, tail_cost),
            (average_runtime, tail_runtime),
            (average_carbon, tail_carbon),
        ) = self._calculate_cost_of_deployment(
            average_node_weights, tail_node_weights, average_edge_weights, tail_edge_weights, deployment
        )

        return (
            deployment,
            (average_cost, tail_cost),
            (average_runtime, tail_runtime),
            (average_carbon, tail_carbon),
            (average_node_weights, tail_node_weights),
            (average_edge_weights, tail_edge_weights),
        )

    def _calculate_cost_of_deployment(
        self,
        average_node_weights: np.ndarray,
        tail_node_weights: np.ndarray,
        average_edge_weights: np.ndarray,
        tail_edge_weights: np.ndarray,
        deployment: dict[int, int],
    ) -> tuple[tuple[float, float], tuple[float, float], tuple[float, float]]:
        (
            average_cost,
            average_runtime,
            average_carbon,
        ) = self._calculate_cost_of_deployment_case(
            average_node_weights, average_edge_weights, deployment, average=True
        )

        (
            tail_cost,
            tail_runtime,
            tail_carbon,
        ) = self._calculate_cost_of_deployment_case(tail_node_weights, tail_edge_weights, deployment)

        return (
            (average_cost, tail_cost),
            (average_runtime, tail_runtime),
            (average_carbon, tail_carbon),
        )

    def _calculate_cost_of_deployment_case(
        self, node_weights: np.ndarray, edge_weights: np.ndarray, deployment: dict[int, int], average: bool = True
    ) -> tuple[float, float, float]:
        initial_node_region = deployment[self._first_instance_index]
        start_hop_transmission_cost = 0
        start_hop_transmission_runtime = 0
        start_hop_transmission_carbon = 0
        if average:
            start_hop_transmission_cost = self._home_region_transmission_costs_average[0, initial_node_region]
            start_hop_transmission_runtime = self._home_region_transmission_costs_average[1, initial_node_region]
            start_hop_transmission_carbon = self._home_region_transmission_costs_average[2, initial_node_region]
        else:
            start_hop_transmission_cost = self._home_region_transmission_costs_tail[0, initial_node_region]
            start_hop_transmission_runtime = self._home_region_transmission_costs_tail[1, initial_node_region]
            start_hop_transmission_carbon = self._home_region_transmission_costs_tail[2, initial_node_region]

        cost = np.sum(node_weights[0]) + np.sum(edge_weights[0]) + start_hop_transmission_cost  # type: ignore
        runtime = self._most_expensive_path(edge_weights[1], node_weights[1]) + start_hop_transmission_runtime  # type: ignore  # pylint: disable=line-too-long
        carbon = np.sum(node_weights[2]) + np.sum(edge_weights[2]) + start_hop_transmission_carbon  # type: ignore

        return cost, runtime, carbon

    def _most_expensive_path(self, edge_weights: np.ndarray, node_weights: np.ndarray) -> float:
        topological_order = self._dag.topological_sort()
        dist = np.full(len(topological_order), -np.inf)
        dist[topological_order[0]] = node_weights[topological_order[0]]

        for node in topological_order:
            outgoing_edges = edge_weights[node, :] != 0
            dist[outgoing_edges] = np.maximum(
                dist[outgoing_edges], dist[node] + edge_weights[node, outgoing_edges] + node_weights[outgoing_edges]
            )

        max_cost: float = np.max(dist)
        return max_cost

    def _get_permitted_region_indices(self, regions: list[dict], instance: int) -> list[int]:
        if instance in self._permitted_region_indices_cache:
            return self._permitted_region_indices_cache[instance]

        permitted_regions: list[dict[(str, str)]] = self._filter_regions_instance(regions, instance)
        if len(permitted_regions) == 0:  # Should never happen in a valid DAG
            raise ValueError("There are no permitted regions for this instance")

        all_regions_indices = self._region_indexer.get_value_indices()
        permitted_regions_indices = [
            all_regions_indices[(region["provider"], region["region"])] for region in permitted_regions
        ]
        self._permitted_region_indices_cache[instance] = permitted_regions_indices
        return permitted_regions_indices
