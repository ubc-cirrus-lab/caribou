import unittest
from multiprocessing import Queue, Process
from unittest.mock import MagicMock, patch, Mock

from caribou.deployment_solver.deployment_metrics_calculator.simple_deployment_metrics_calculator import (
    SimpleDeploymentMetricsCalculator,
)


def mock_simulation_worker(
    n_iterations: int,
    input_queue: Queue,
    output_queue: Queue,
):
    while True:
        received_input = input_queue.get()
        if isinstance(received_input, str) or received_input is None:
            output_queue.put("OK")
            continue
        costs_distribution_list: list[float] = []
        runtimes_distribution_list: list[float] = []
        carbons_distribution_list: list[float] = []
        for _ in range(n_iterations):
            results = {"cost": 1.0, "runtime": 1.0, "carbon": 1.0}
            costs_distribution_list.append(results["cost"])
            runtimes_distribution_list.append(results["runtime"])
            carbons_distribution_list.append(results["carbon"])
        output_queue.put(
            (
                costs_distribution_list,
                runtimes_distribution_list,
                carbons_distribution_list,
            )
        )


def mock_init_workers(*args, **kwargs):
    n_processes = args[5]
    n_iterations = args[6]
    input_queue = args[7]
    output_queue = args[8]
    pool = []
    for _ in range(n_processes):
        p = Process(
            target=mock_simulation_worker,
            args=(
                n_iterations,
                input_queue,
                output_queue,
            ),
        )
        p.start()
        pool.append(p)
    return pool


class TestSimpleDeploymentMetricsCalculator(unittest.TestCase):
    @patch.object(
        SimpleDeploymentMetricsCalculator,
        "calculate_workflow",
        return_value={"cost": 1.0, "runtime": 1.0, "carbon": 1.0},
    )
    def test_perform_monte_carlo_simulation(self, mock_calculate_workflow):
        # Setup
        self.calculator = SimpleDeploymentMetricsCalculator(
            MagicMock(), MagicMock(), MagicMock(), MagicMock(), n_processes=1
        )

        # Call the method with a test deployment
        deployment = [0, 1, 2, 3]
        results = self.calculator._perform_monte_carlo_simulation(deployment)

        # Verify the results
        self.assertEqual(results["average_cost"], 1.0)
        self.assertEqual(results["average_runtime"], 1.0)
        self.assertEqual(results["average_carbon"], 1.0)
        self.assertEqual(results["tail_cost"], 1.0)
        self.assertEqual(results["tail_runtime"], 1.0)
        self.assertEqual(results["tail_carbon"], 1.0)

        # Verify that the mock method was called the correct number of times with the correct arguments
        self.assertEqual(mock_calculate_workflow.call_count, 2000)
        mock_calculate_workflow.assert_called_with(deployment)

    @patch.object(
        SimpleDeploymentMetricsCalculator,
        "_init_workers",
        side_effect=mock_init_workers,
    )
    def test_perform_monte_carlo_simulation_parallel(self, mock_init_workers):
        # Setup
        self.calculator = SimpleDeploymentMetricsCalculator(
            MagicMock(), MagicMock(), MagicMock(), MagicMock(), n_processes=4
        )

        # Call the method with a test deployment
        deployment = [0, 1, 2, 3]
        results = self.calculator._perform_monte_carlo_simulation(deployment)

        # Verify the results
        self.assertEqual(results["average_cost"], 1.0)
        self.assertEqual(results["average_runtime"], 1.0)
        self.assertEqual(results["average_carbon"], 1.0)
        self.assertEqual(results["tail_cost"], 1.0)
        self.assertEqual(results["tail_runtime"], 1.0)
        self.assertEqual(results["tail_carbon"], 1.0)


if __name__ == "__main__":
    unittest.main()
