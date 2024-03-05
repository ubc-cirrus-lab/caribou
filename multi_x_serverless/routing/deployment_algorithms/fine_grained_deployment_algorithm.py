from itertools import product
from multiprocessing import Pool
from typing import Optional

from multi_x_serverless.routing.deployment_algorithms.deployment_algorithm import DeploymentAlgorithm


class FineGrainedDeploymentAlgorithm(DeploymentAlgorithm):
    def _run_algorithm(self) -> list[tuple[list[int], dict[str, float]]]:
        deployments = self._generate_all_possible_fine_deployments()
        return deployments

    def _generate_all_possible_fine_deployments(self) -> list[tuple[list[int], dict[str, float]]]:
        deployments = []
        all_combinations = product(
            self._region_indexer.get_value_indices().values(),
            repeat=self._number_of_instances,
        )
        for deployment_tuple in all_combinations:
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
        deployment_metrics: dict[str, float] = self._deployment_metrics_calculator.calculate_deployment_metrics(
            deployment
        )

        if self._is_hard_constraint_failed(deployment_metrics):
            return None

        return (deployment, deployment_metrics)
