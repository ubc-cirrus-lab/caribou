from multi_x_serverless.routing.deployment_algorithms.deployment_algorithm import DeploymentAlgorithm

from itertools import product


class FineGrainedDeploymentAlgorithm(DeploymentAlgorithm):
    def _run_algorithm(self) -> list[tuple[dict[int, int], dict[str, float]]]:
        deployments = self._generate_all_possible_fine_deployments()
        return deployments

    def _generate_all_possible_fine_deployments(self) -> list[tuple[dict[int, int], dict[str, float]]]:
        deployments = []

        # Generate every possible combination of region and instances
        all_combinations = product(
            self._region_indexer.get_value_indices().values(),
            repeat=len(self._instance_indexer.get_value_indices().values()),
        )

        for combination in all_combinations:
            # Map each combination to the corresponding instance
            deployment = {
                instance: region
                for instance, region in zip(self._instance_indexer.get_value_indices().values(), combination)
            }
            # Calculate the deployment metrics for the mapping
            deployment_metrics = self._deployment_metrics_calculator.calculate_deployment_metrics(deployment)

            deployments.append(deployment, deployment_metrics)

        return deployments
