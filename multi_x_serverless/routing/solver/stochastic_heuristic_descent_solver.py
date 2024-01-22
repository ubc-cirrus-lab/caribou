import random

from multi_x_serverless.routing.solver.solver import Solver
from multi_x_serverless.routing.workflow_config import WorkflowConfig


class StochasticHeuristicDescentSolver(Solver):
    def __init__(self, workflow_config: WorkflowConfig) -> None:
        super().__init__(workflow_config)
        self._max_iterations = 1000
        self._learning_rate = 0.01
        self._positive_regions: set[int] = set()
        self._bias_probability = 0.5
        self._topological_order = self._dag.topological_sort()

    def _solve(self, regions: list[dict]) -> list[tuple[dict[int, int], float, float, float]]:
        deployments = []
        current_deployment = self._init_deployment()

        for _ in range(self._max_iterations):
            num_instances_to_update = int(len(current_deployment[0]) * self._learning_rate)

            # Check if the current deployment is valid
            if not self._fail_hard_resource_constraints(
                self._workflow_config.constraints, current_deployment[1], current_deployment[2], current_deployment[3]
            ):
                deployments.append(
                    (current_deployment[0].copy(), current_deployment[1], current_deployment[2], current_deployment[3])
                )

            # Select a random deployment to update
            next_deployment = random.choice(deployments)

            # For the number of instances to update, select a random instance and region
            for _ in range(num_instances_to_update):
                instance, new_region = self.select_random_instance_and_region(next_deployment[0], regions)

                is_improvement, cost, runtime, carbon = self._is_improvement(next_deployment, instance, new_region)
                if is_improvement:
                    next_deployment[0][instance] = new_region
                    self._record_successful_change(instance, new_region)
                    current_deployment = (next_deployment[0], cost, runtime, carbon)

        return deployments

    def _init_deployment(self) -> tuple[dict[int, int], float, float, float]:
        # TODO (#14): Implement this function
        return {}  # type: ignore

    def _calculate_costs_of_deployment(self, deployment: dict[int, int]) -> tuple[float, float, float]:
        # TODO (#14): Implement this function
        return (0, 0, 0)

    def select_random_instance_and_region(
        self, previous_deployment: dict[int, int], regions: list[dict]
    ) -> tuple[int, int]:
        instance = random.choice(list(previous_deployment.keys()))
        if random.random() < self._bias_probability:
            new_region = random.choice(list(self._positive_regions))
            if new_region != previous_deployment[instance]:
                return instance, new_region

        permitted_regions: list[dict[(str, str)]] = self._filter_regions_instance(regions, instance)
        if len(permitted_regions) == 0:  # Should never happen in a valid DAG
            raise Exception("There are no permitted regions for this instance")

        all_regions_indices = self._region_indexer.get_value_indices()
        permitted_regions_indices = [
            all_regions_indices[(region["provider"], region["region"])] for region in permitted_regions
        ]

        new_region = random.choice(permitted_regions_indices)
        return instance, new_region

    def _is_improvement(
        self, deployment: tuple[dict[int, int], float, float, float], instance: int, new_region: int
    ) -> tuple[bool, float, float, float]:
        cost, runtime, carbon = self._calculate_costs_of_deployment(deployment[0])

        if any(
            [
                cost < deployment[1],
                runtime < deployment[2],
                carbon < deployment[3],
            ]
        ):  # If any of the costs are better than the current deployment
            return True, cost, runtime, carbon
        return False, cost, runtime, carbon

    def _record_successful_change(self, instance: int, new_region: int) -> None:
        if new_region not in self._positive_regions:
            self._positive_regions.add(new_region)
