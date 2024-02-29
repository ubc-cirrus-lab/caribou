import random

from multi_x_serverless.routing.deployment_metrics_calculator.deployment_metrics_calculator import (
    DeploymentMetricsCalculator,
)

# from unittest.mock import MagicMock


class SimpleDeploymentMetricsCalculator(DeploymentMetricsCalculator):
    def calculate_deployment_metrics(self, deployment: list[int]) -> dict[str, float]:
        average_cost, average_runtime, average_carbon = self._calculate_workflow(deployment, False).values()
        # tail_cost, tail_runtime, tail_carbon = self._calculate_workflow(deployment, True).values()
        tail_cost, tail_runtime, tail_carbon = 0.0, 0.0, 0.0

        return {
            "average_cost": average_cost,
            "average_runtime": average_runtime,
            "average_carbon": average_carbon,
            "tail_cost": tail_cost,
            "tail_runtime": tail_runtime,
            "tail_carbon": tail_carbon,
        }

    def _calculate_workflow(self, deployment: list[int], probabilistic_case: bool) -> dict[str, float]:
        total_cost = 0.0
        total_carbon = 0.0

        # Keep track of instances of the node that will get invoked in this round.
        invoked_instance_set: set = set([0])

        # Keep track of the runtime of the instances that were invoked in this round.

        for instance_index, region_index in enumerate(deployment):
            if instance_index in invoked_instance_set:  # Only care about the invoked instances
                cost, _, carbon = self._input_manager.get_execution_cost_carbon_runtime(
                    instance_index, region_index, probabilistic_case
                )
                total_cost += cost
                total_carbon += carbon
                invoked_instance_set.add(instance_index)
                print(f"Instance {instance_index} is deployed in region {region_index}")

                # Get the next instance to be invoked

        # At this point we may have 1 or more leaf nodes, we need to get the max runtime from them.

        return {
            "cost": 0.0,
            "runtime": 0.0,
            "carbon": 0.0,
        }

    def is_invoked(self, from_instance_index: int, to_instance_index: int, probabilistic_case: bool) -> bool:
        """
        Return true if the edge would be triggered, if the probabilistic_case is True,
        It triggers dependent on the probability of the edge, if the probabilistic_case is False,
        It always triggers the edge.
        """
        if not probabilistic_case:
            return True

        invocation_probability = self._input_manager.get_invocation_probability(from_instance_index, to_instance_index)
        return random.random() < invocation_probability


# if __name__ == "__main__":
#     deployment_calculator = SimpleDeploymentMetricsCalculator(MagicMock(), MagicMock())
#     deployment_calculator.calculate_deployment_metrics([1, 2, 3, 4])
#     # deployment_metrics_calculator = DeploymentMetricsCalculator()
#     # deployment_metrics_calculator.calculate_deployment_metrics(deployment
