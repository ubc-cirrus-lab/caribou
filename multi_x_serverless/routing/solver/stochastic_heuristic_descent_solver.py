import random
from typing import Optional

import numpy as np

from multi_x_serverless.routing.solver.input.input_manager import InputManager
from multi_x_serverless.routing.solver.solver import Solver
from multi_x_serverless.routing.workflow_config import WorkflowConfig


class StochasticHeuristicDescentSolver(Solver):
    def __init__(
        self,
        workflow_config: WorkflowConfig,
        all_available_regions: Optional[list[str]] = None,
        input_manager: Optional[InputManager] = None,
    ) -> None:
        super().__init__(workflow_config, all_available_regions, input_manager)
        self._max_iterations = len(self._worklow_level_permitted_regions) * 100
        self._learning_rate = 0.1
        self._positive_regions: set[int] = set()
        self._bias_probability = 0.5

        if len(self._topological_order) == 0:
            raise ValueError("The DAG has no nodes")

    def _solve(self, regions: list[str]) -> list[tuple[dict[int, int], float, float, float]]:
        average_case_deployments: list[tuple[dict[int, int], float, float, float]] = []
        current_deployment = self._init_deployment()

        for _ in range(self._max_iterations):
            num_instances_to_update = int(len(current_deployment[0]) * self._learning_rate) + 1

            # Check if the current deployment is valid
            if (
                not self._fail_hard_resource_constraints(
                    self._workflow_config.constraints,
                    current_deployment[1][1],
                    current_deployment[2][1],
                    current_deployment[3][1],
                )
                and (
                    current_deployment[0].copy(),
                    current_deployment[1][0],
                    current_deployment[2][0],
                    current_deployment[3][0],
                )
                not in average_case_deployments
            ):
                average_case_deployments.append(
                    (
                        current_deployment[0].copy(),
                        current_deployment[1][0],
                        current_deployment[2][0],
                        current_deployment[3][0],
                    )
                )

            # For the number of instances to update, select a random instance and region
            for _ in range(num_instances_to_update):
                selected_instance, new_region = self.select_random_instance_and_region(current_deployment[0], regions)

                if selected_instance == -1 or new_region == -1:
                    continue

                (
                    is_improvement,
                    cost_tuple,
                    runtime_tuple,
                    carbon_tuple,
                    node_weights_tuple,
                    edge_weight_tuple,
                ) = self._is_improvement(current_deployment, selected_instance, new_region)
                if is_improvement:
                    current_deployment[0][selected_instance] = new_region
                    self._record_successful_change(new_region)
                    current_deployment = (
                        current_deployment[0],
                        cost_tuple,
                        runtime_tuple,
                        carbon_tuple,
                        node_weights_tuple,
                        edge_weight_tuple,
                    )

        return average_case_deployments

    def _init_deployment(
        self,
    ) -> tuple[
        dict[int, int],
        tuple[float, float],
        tuple[float, float],
        tuple[float, float],
        tuple[np.ndarray, np.ndarray],
        tuple[np.ndarray, np.ndarray],
    ]:
        return self.init_deployment_to_region(self._home_region_index)

    def select_random_instance_and_region(
        self, previous_deployment: dict[int, int], regions: list[str]
    ) -> tuple[int, int]:
        instance = random.choice(list(previous_deployment.keys()))

        permitted_regions_indices = self._get_permitted_region_indices(regions, instance)

        if random.random() < self._bias_probability and len(self._positive_regions) > 0:
            new_region = random.choice(list(self._positive_regions))
            if new_region != previous_deployment[instance] and new_region in permitted_regions_indices:
                return instance, new_region

        new_region = random.choice(permitted_regions_indices)

        if new_region != previous_deployment[instance]:
            return instance, new_region

        return -1, -1

    def _calculate_updated_costs_of_deployment(
        self,
        previous_deployment: tuple[
            dict[int, int],
            tuple[float, float],
            tuple[float, float],
            tuple[float, float],
            tuple[np.ndarray, np.ndarray],
            tuple[np.ndarray, np.ndarray],
        ],
        selected_instance: int,
        new_region: int,
        new_average_node_weights: np.ndarray,
        new_tail_node_weights: np.ndarray,
        new_average_edge_weights: np.ndarray,
        new_tail_edge_weights: np.ndarray,
    ) -> tuple[tuple[float, float], tuple[float, float], tuple[float, float]]:
        (
            average_execution_cost,
            average_execution_carbon,
            average_execution_runtime,
        ) = self._input_manager.get_execution_cost_carbon_runtime(
            selected_instance, new_region, consider_probabilistic_invocations=True
        )

        new_average_node_weights[0, selected_instance] = average_execution_cost
        new_average_node_weights[1, selected_instance] = average_execution_runtime
        new_average_node_weights[2, selected_instance] = average_execution_carbon

        (
            tail_execution_cost,
            tail_execution_carbon,
            tail_execution_runtime,
        ) = self._input_manager.get_execution_cost_carbon_runtime(selected_instance, new_region)

        new_tail_node_weights[0, selected_instance] = tail_execution_cost
        new_tail_node_weights[1, selected_instance] = tail_execution_runtime
        new_tail_node_weights[2, selected_instance] = tail_execution_carbon

        for i, j in zip(self._adjacency_indexes[0], self._adjacency_indexes[1]):
            if i == selected_instance and j != selected_instance:
                from_region = new_region
                to_region = previous_deployment[0][j]
            elif i != selected_instance and j == selected_instance:
                from_region = previous_deployment[0][i]
                to_region = new_region
            elif i == selected_instance and j == selected_instance:
                from_region = new_region
                to_region = new_region
            else:
                continue

            (
                average_transmission_cost,
                average_transmission_carbon,
                average_transmission_runtime,
            ) = self._input_manager.get_transmission_cost_carbon_runtime(
                i, j, from_region, to_region, consider_probabilistic_invocations=True
            )

            new_average_edge_weights[0, i, j] = average_transmission_cost
            new_average_edge_weights[1, i, j] = average_transmission_runtime
            new_average_edge_weights[2, i, j] = average_transmission_carbon

            (
                tail_transmission_cost,
                tail_transmission_carbon,
                tail_transmission_runtime,
            ) = self._input_manager.get_transmission_cost_carbon_runtime(i, j, from_region, to_region)

            new_tail_edge_weights[0, i, j] = tail_transmission_cost
            new_tail_edge_weights[1, i, j] = tail_transmission_runtime
            new_tail_edge_weights[2, i, j] = tail_transmission_carbon

        # We only need the deployment if the selected instance is the first instance
        if selected_instance == self._first_instance_index:
            new_deployments = previous_deployment[0].copy()
            new_deployments[selected_instance] = new_region
        else:
            new_deployments = previous_deployment[0]

        (
            (average_cost, tail_cost),
            (average_runtime, tail_runtime),
            (average_carbon, tail_carbon),
        ) = self._calculate_cost_of_deployment(
            new_average_node_weights,
            new_tail_node_weights,
            new_average_edge_weights,
            new_tail_edge_weights,
            new_deployments,
        )
        return (
            (average_cost, tail_cost),
            (average_runtime, tail_runtime),
            (average_carbon, tail_carbon),
        )

    def _is_improvement(
        self,
        deployment: tuple[
            dict[int, int],
            tuple[float, float],
            tuple[float, float],
            tuple[float, float],
            tuple[np.ndarray, np.ndarray],
            tuple[np.ndarray, np.ndarray],
        ],
        selected_instance: int,
        new_region: int,
    ) -> tuple[
        bool,
        tuple[float, float],
        tuple[float, float],
        tuple[float, float],
        tuple[np.ndarray, np.ndarray],
        tuple[np.ndarray, np.ndarray],
    ]:
        new_average_node_weights = deployment[4][0].copy()
        new_tail_node_weights = deployment[4][1].copy()
        new_average_edge_weights = deployment[5][0].copy()
        new_tail_edge_weights = deployment[5][1].copy()

        (
            (average_cost, tail_cost),
            (average_runtime, tail_runtime),
            (average_carbon, tail_carbon),
        ) = self._calculate_updated_costs_of_deployment(
            deployment,
            selected_instance,
            new_region,
            new_average_node_weights,
            new_tail_node_weights,
            new_average_edge_weights,
            new_tail_edge_weights,
        )

        if self._objective_function.calculate(
            cost=average_cost,
            runtime=average_runtime,
            carbon=average_carbon,
            best_cost=deployment[1][0],
            best_runtime=deployment[2][0],
            best_carbon=deployment[3][0],
        ):
            return (
                True,
                (average_cost, tail_cost),
                (average_runtime, tail_runtime),
                (average_carbon, tail_carbon),
                (new_average_node_weights, new_tail_node_weights),
                (new_average_edge_weights, new_tail_edge_weights),
            )
        return (
            False,
            (average_cost, tail_cost),
            (average_runtime, tail_runtime),
            (average_carbon, tail_carbon),
            (new_average_node_weights, new_tail_node_weights),
            (new_average_edge_weights, new_tail_edge_weights),
        )

    def _record_successful_change(self, new_region: int) -> None:
        if new_region not in self._positive_regions:
            self._positive_regions.add(new_region)

    def _most_expensive_path(self, edge_weights: np.ndarray, node_weights: np.ndarray) -> float:
        dist = np.full(len(self._topological_order), -np.inf)
        dist[self._topological_order[0]] = node_weights[self._topological_order[0]]

        for node in self._topological_order:
            outgoing_edges = edge_weights[node, :] != 0
            dist[outgoing_edges] = np.maximum(
                dist[outgoing_edges], dist[node] + edge_weights[node, outgoing_edges] + node_weights[outgoing_edges]
            )

        max_cost: float = np.max(dist)
        return max_cost
