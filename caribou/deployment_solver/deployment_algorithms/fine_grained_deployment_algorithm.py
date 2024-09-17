import time
from itertools import product
from typing import Optional

from caribou.deployment_solver.deployment_algorithms.deployment_algorithm import DeploymentAlgorithm


class FineGrainedDeploymentAlgorithm(DeploymentAlgorithm):
    def _run_algorithm(self, timeout: float = float("inf")) -> list[tuple[list[int], dict[str, float]]]:
        deployments = self._generate_all_possible_fine_deployments(timeout)
        return deployments

    def _generate_all_possible_fine_deployments(
        self, timeout: float = float("inf")
    ) -> list[tuple[list[int], dict[str, float]]]:
        deployments = []
        all_combinations = product(
            self._region_indexer.get_value_indices().values(),
            repeat=self._number_of_instances,
        )
        start_time = time.time()
        for deployment_tuple in all_combinations:
            if (time.time() - start_time) > timeout:
                break
            deployment = self._generate_and_check_deployment(deployment_tuple)
            if deployment is not None:
                deployments.append(deployment)
        return deployments

    def _generate_and_check_deployment(
        self, deployment_tuple: tuple[int, ...]
    ) -> Optional[tuple[list[int], dict[str, float]]]:
        if any(
            deployment_tuple[instance] not in self._per_instance_permitted_regions[instance]
            for instance in range(self._number_of_instances)
        ):
            return None

        # Type matching
        deployment: list[int] = list(deployment_tuple)

        # Calculate the deployment metrics for the mapping
        if deployment == self._home_deployment:
            deployment_metrics = self._home_deployment_metrics
        else:
            deployment_metrics = self._deployment_metrics_calculator.calculate_deployment_metrics(deployment)

        if self._is_hard_constraint_failed(deployment_metrics):
            return None

        return (deployment, deployment_metrics)
