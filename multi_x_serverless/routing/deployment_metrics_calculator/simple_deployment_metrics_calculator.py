from multi_x_serverless.routing.deployment_metrics_calculator.deployment_metrics_calculator import (
    DeploymentMetricsCalculator,
)


class SimpleDeploymentMetricsCalculator(DeploymentMetricsCalculator):
    def _perform_monte_carlo_simulation(self, deployment: list[int], times: int) -> dict[str, float]:
        """
        Perform a Monte Carlo simulation to get the average cost, runtime, and carbon footprint of the deployment.
        """
        total_cost = 0.0
        total_runtime = 0.0
        total_carbon = 0.0

        for _ in range(times):
            results = self._calculate_workflow(deployment, True)
            total_cost += results["cost"]
            total_runtime += results["runtime"]
            total_carbon += results["carbon"]

        return {
            "cost": total_cost / times,
            "runtime": total_runtime / times,
            "carbon": total_carbon / times,
        }
