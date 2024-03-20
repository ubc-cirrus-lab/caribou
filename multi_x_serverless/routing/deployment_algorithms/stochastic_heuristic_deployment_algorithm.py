import random

from multi_x_serverless.routing.deployment_algorithms.deployment_algorithm import DeploymentAlgorithm
from multi_x_serverless.routing.workflow_config import WorkflowConfig


class StochasticHeuristicDeploymentAlgorithm(DeploymentAlgorithm):
    def __init__(self, workflow_config: WorkflowConfig, expiry_time_delta_seconds: int = 604800) -> None:
        super().__init__(workflow_config, expiry_time_delta_seconds)
        self._setup()

    def _setup(self) -> None:
        # This learning rate should be an int -> Convert the float to an int
        self._learning_rate: int = int(self._number_of_instances * 0.2 + 1)
        self._num_iterations = (
            len(self._region_indexer.get_value_indices().values())
            * len(self._instance_indexer.get_value_indices().values())
            * 6
        )
        self._temperature = 1.0
        self._bias_regions: set[int] = set()
        self._bias_probability = 0.2
        self._max_number_combinations = (
            len(self._region_indexer.get_value_indices().values()) ** self._number_of_instances
        )

    def _run_algorithm(self) -> list[tuple[list[int], dict[str, float]]]:
        deployments = self._generate_stochastic_heuristic_deployments()
        return deployments

    def _generate_stochastic_heuristic_deployments(self) -> list[tuple[list[int], dict[str, float]]]:
        current_deployment_metrics = self._home_deployment_metrics.copy()
        current_deployment = self._home_deployment.copy()

        deployments = []
        generated_deployments: set[tuple[int, ...]] = set()
        for _ in range(self._num_iterations):
            new_deployment = self._generate_new_deployment(current_deployment)
            if tuple(new_deployment) in generated_deployments:
                continue
            new_deployment_metrics = self._deployment_metrics_calculator.calculate_deployment_metrics(new_deployment)

            if self._is_hard_constraint_failed(new_deployment_metrics):
                continue

            if self._is_improvement(
                current_deployment_metrics, new_deployment_metrics, new_deployment, current_deployment
            ):
                current_deployment = new_deployment
                current_deployment_metrics = new_deployment_metrics
                deployments.append((current_deployment, current_deployment_metrics))
                generated_deployments.add(tuple(current_deployment))

            self._temperature *= 0.99

            if len(deployments) >= self._max_number_combinations:
                break
        return deployments

    def _store_bias_regions(self, new_deployment: list[int], current_deployment: list[int]) -> None:
        for instance, new_region in enumerate(new_deployment):
            if new_region != current_deployment[instance]:
                self._bias_regions.add(new_region)

    def _is_improvement(
        self,
        current_deployment_metrics: dict[str, float],
        new_deployment_metrics: dict[str, float],
        new_deployment: list[int],
        current_deployment: list[int],
    ) -> bool:
        if (
            new_deployment_metrics[self._ranker.number_one_priority]
            < current_deployment_metrics[self._ranker.number_one_priority]
        ):
            self._store_bias_regions(new_deployment, current_deployment)
            return True
        return random.random() < self._acceptance_probability()

    def _acceptance_probability(self) -> float:
        # Acceptance probability is calculated using the Boltzmann distribution
        return (
            1.0
            if self._temperature == 0
            else min(
                1.0,
                2.0
                ** (
                    -abs(self._home_deployment_metrics[self._ranker.number_one_priority] - self._temperature)
                    / self._temperature
                ),
            )
        )

    def _generate_new_deployment(self, current_deployment: list[int]) -> list[int]:
        new_deployment = current_deployment.copy()
        instances_to_move = [random.randint(0, self._number_of_instances - 1) for _ in range(self._learning_rate)]
        for instance in instances_to_move:
            new_deployment[instance] = self._choose_new_region(instance)
        return new_deployment

    def _choose_new_region(self, instance: int) -> int:
        permitted_regions = self._per_instance_permitted_regions[instance]
        if random.random() < self._bias_probability and len(self._bias_regions) > 0:
            return self._choose_biased_region(permitted_regions)
        return random.choice(permitted_regions)

    def _choose_biased_region(self, permitted_regions: list[int]) -> int:
        possible_bias_regions = self._bias_regions.intersection(permitted_regions)
        if len(possible_bias_regions) > 0:
            return random.choice(list(possible_bias_regions))
        return random.choice(permitted_regions)
