import numpy as np

from multi_x_serverless.routing.deployment_metrics_calculator.deployment_metrics_calculator import (
    DeploymentMetricsCalculator,
)


class SimpleDeploymentMetricsCalculator(DeploymentMetricsCalculator):
    def _perform_monte_carlo_simulation(self, deployment: list[int], times: int) -> dict[str, float]:
        """
        Perform a Monte Carlo simulation to both the average and tail
        cost, runtime, and carbon footprint of the deployment.
        """
        costs_distribution_list: list[float] = []
        runtimes_distribution_list: list[float] = []
        carbons_distribution_list: list[float] = []

        for _ in range(times):
            results = self._calculate_workflow(deployment, True)
            costs_distribution_list.append(results["cost"])
            runtimes_distribution_list.append(results["runtime"])
            carbons_distribution_list.append(results["carbon"])

        # Sort and convert to numpy arrays
        costs_distribution: np.ndarray = np.array(costs_distribution_list)
        runtimes_distribution: np.ndarray = np.array(runtimes_distribution_list)
        carbons_distribution: np.ndarray = np.array(carbons_distribution_list)

        return {
            "average_cost": float(np.mean(costs_distribution)),
            "average_runtime": float(np.mean(runtimes_distribution)),
            "average_carbon": float(np.mean(carbons_distribution)),
            "tail_cost": float(np.percentile(costs_distribution, self._tail_latency_threshold)),
            "tail_runtime": float(np.percentile(runtimes_distribution, self._tail_latency_threshold)),
            "tail_carbon": float(np.percentile(carbons_distribution, self._tail_latency_threshold)),
        }
