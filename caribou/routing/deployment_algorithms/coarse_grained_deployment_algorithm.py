from typing import Optional

from caribou.routing.deployment_algorithms.deployment_algorithm import DeploymentAlgorithm


class CoarseGrainedDeploymentAlgorithm(DeploymentAlgorithm):
    def _run_algorithm(self) -> list[tuple[list[int], dict[str, float]]]:
        deployments = self._generate_all_possible_coarse_deployments()
        return deployments

    def _generate_all_possible_coarse_deployments(self) -> list[tuple[list[int], dict[str, float]]]:
        deployments = []
        for index_value in self._region_indexer.get_value_indices().values():
            deployment = self._generate_and_check_deployment(index_value)
            if deployment is not None:
                deployments.append(deployment)
        return deployments

    def _generate_and_check_deployment(self, region_index: int) -> Optional[tuple[list[int], dict[str, float]]]:
        if any(
            region_index not in self._per_instance_permitted_regions[instance_index]
            for instance_index in range(self._number_of_instances)
        ):
            return None
        deployment = self._generate_deployment(region_index)
        deployment_metrics = self._deployment_metrics_calculator.calculate_deployment_metrics(deployment)

        if self._is_hard_constraint_failed(deployment_metrics):
            return None

        return (deployment, deployment_metrics)

    def _generate_deployment(self, region_index: int) -> list[int]:
        return [region_index for _ in range(self._number_of_instances)]