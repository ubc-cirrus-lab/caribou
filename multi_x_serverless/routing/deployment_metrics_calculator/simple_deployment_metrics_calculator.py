import numpy as np
import scipy.stats as st

from multi_x_serverless.routing.deployment_metrics_calculator.deployment_metrics_calculator import (
    DeploymentMetricsCalculator,
)


class SimpleDeploymentMetricsCalculator(DeploymentMetricsCalculator):
    def _perform_monte_carlo_simulation(self, deployment: list[int]) -> dict[str, float]:
        """
        Perform a Monte Carlo simulation to both the average and tail
        cost, runtime, and carbon footprint of the deployment.
        """
        costs_distribution_list: list[float] = []
        runtimes_distribution_list: list[float] = []
        carbons_distribution_list: list[float] = []

        max_number_of_iterations = 20000
        number_of_iterations = 0
        threshold = 0.05
        batch_size = 1000

        while number_of_iterations < max_number_of_iterations:
            for _ in range(batch_size):
                results = self._calculate_workflow(deployment)
                costs_distribution_list.append(results["cost"])
                runtimes_distribution_list.append(results["runtime"])
                carbons_distribution_list.append(results["carbon"])

            number_of_iterations += batch_size

            all_within_threshold = True

            for distribution in [costs_distribution_list, runtimes_distribution_list, carbons_distribution_list]:
                mean = np.mean(distribution)
                if len(distribution) > 1 and np.std(distribution, ddof=1) > 0:
                    ci_low, ci_up = st.t.interval(
                        1 - threshold, len(distribution) - 1, loc=mean, scale=st.sem(distribution)
                    )
                    ci_width = ci_up - ci_low
                    relative_ci_width = ci_width / mean if mean else float("inf")
                else:
                    if all_within_threshold:
                        break

                if relative_ci_width > threshold:
                    all_within_threshold = False
                    break

            if all_within_threshold or number_of_iterations >= max_number_of_iterations:
                break

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
