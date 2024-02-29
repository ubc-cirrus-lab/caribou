from multi_x_serverless.routing.deployment_algorithms.deployment_algorithm import DeploymentAlgorithm


class CoarseGrainedDeploymentAlgorithm(DeploymentAlgorithm):
    def _run_algorithm(self) -> list[tuple[dict[int, int], dict[str, float]]]:
        deployments = self._generate_all_possible_deployments()
        return deployments

    def _generate_all_possible_coarse_deployments(self) -> list[tuple[dict[int, int], dict[str, float]]]:
        deployments = []
        for region_index in self._region_indexer.get_value_indices().values():
            deployment = self._generate_deployment(region_index)
            deployment_metrics = self._deployment_metrics_calculator.calculate_deployment_metrics(deployment)
            deployments.append((deployment, deployment_metrics))
        return deployments

    def _generate_deployment(self, region_index: int) -> dict[int, int]:
        return {instance_index: region_index for instance_index in self._instance_indexer.get_value_indices().values()}
