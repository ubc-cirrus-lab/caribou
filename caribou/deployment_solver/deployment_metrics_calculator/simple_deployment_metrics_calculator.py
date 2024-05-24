import statistics

import numpy as np
import scipy.stats as st

from caribou.deployment_solver.deployment_metrics_calculator.deployment_metrics_calculator import (
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

        max_number_of_iterations = 2000
        number_of_iterations = 0
        threshold = 0.05
        batch_size = 200

        while number_of_iterations < max_number_of_iterations:
            for _ in range(batch_size):
                results = self.calculate_workflow(deployment)
                costs_distribution_list.append(results["cost"])
                runtimes_distribution_list.append(results["runtime"])
                carbons_distribution_list.append(results["carbon"])

            number_of_iterations += batch_size

            all_within_threshold = True

            for distribution in [runtimes_distribution_list, carbons_distribution_list, costs_distribution_list]:
                mean = np.mean(distribution)
                len_distribution = len(distribution)
                if mean and len_distribution > 1:
                    ci_low, ci_up = st.t.interval(
                        1 - threshold, len_distribution - 1, loc=mean, scale=st.sem(distribution)
                    )
                    ci_width = ci_up - ci_low
                    relative_ci_width = ci_width / mean
                    if relative_ci_width > threshold:
                        all_within_threshold = False
                        break
                elif all_within_threshold:
                    break

        result = {
            "average_cost": float(statistics.mean(costs_distribution_list)),
            "average_runtime": float(statistics.mean(runtimes_distribution_list)),
            "average_carbon": float(statistics.mean(carbons_distribution_list)),
            "tail_cost": float(np.percentile(costs_distribution_list, self._tail_latency_threshold)),
            "tail_runtime": float(np.percentile(runtimes_distribution_list, self._tail_latency_threshold)),
            "tail_carbon": float(np.percentile(carbons_distribution_list, self._tail_latency_threshold)),
        }
        return result