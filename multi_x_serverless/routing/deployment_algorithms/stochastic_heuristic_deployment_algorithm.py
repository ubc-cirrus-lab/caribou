from multi_x_serverless.routing.deployment_algorithms.deployment_algorithm import DeploymentAlgorithm

import random


class StochasticHeuristicDeploymentAlgorithm(DeploymentAlgorithm):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._learning_rate = len(self._instance_indexer.get_value_indices().values()) * 0.1 + 1
        self._num_iterations = len(self._region_indexer.get_value_indices().values()) * 100
        self._temperature = 1.0

    def _run_algorithm(self) -> list[tuple[dict[int, int], dict[str, float]]]:
        deployments = self._generate_stochastic_heuristic_deployments()
        return deployments

    def _generate_stochastic_heuristic_deployments(self) -> list[tuple[dict[int, int], dict[str, float]]]:
        current_deployment_metrics = self._home_deployment_metrics.copy()
        current_deployment = self._home_deployment.copy()

        deployments = []
        for _ in range(self._num_iterations):
            new_deployment = self._generate_new_deployment(current_deployment)
            new_deployment_metrics = self._deployment_metrics_calculator.calculate_deployment_metrics(new_deployment)

            if self._is_hard_constraint_failed(new_deployment_metrics):
                continue

            if self._is_improvement(current_deployment_metrics, new_deployment_metrics):
                current_deployment = new_deployment
                current_deployment_metrics = new_deployment_metrics
                deployments.append((current_deployment, current_deployment_metrics))

            self._temperature *= 0.99

        return deployments

    def _is_improvement(
        self, current_deployment_metrics: dict[str, float], new_deployment_metrics: dict[str, float]
    ) -> bool:
        return (
            new_deployment_metrics[self._ranker.number_one_priority]
            < current_deployment_metrics[self._ranker.number_one_priority]
            or random.random() < self._acceptance_probability()
        )

    def _acceptance_probability(self) -> float:
        # Acceptance probability is calculated using the Boltzmann distribution
        return (
            1.0
            if self._temperature == 0
            else 2.0
            ** (
                -abs(self._home_deployment_metrics[self._ranker.number_one_priority] - self._temperature)
                / self._temperature
            )
        )

    def _generate_new_deployment(self, current_deployment: list[int]) -> list[int]:
        new_deployment = current_deployment.copy()
        instances_to_move = [
            random.choice(self._instance_indexer.get_value_indices().values()) for _ in range(self._learning_rate)
        ]
        for instance in instances_to_move:
            new_deployment[instance] = self._choose_new_region(instance)
        return new_deployment

    def _choose_new_region(self, instance: int) -> int:
        permitted_regions = self._per_instance_permitted_regions[instance]
        return random.choice(permitted_regions)
