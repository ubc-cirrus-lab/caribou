import unittest
from unittest.mock import MagicMock, patch, Mock
from caribou.deployment_solver.deployment_metrics_calculator.simple_deployment_metrics_calculator import (
    SimpleDeploymentMetricsCalculator,
)


class TestSimpleDeploymentMetricsCalculator(unittest.TestCase):
    def setUp(self):
        self.calculator = SimpleDeploymentMetricsCalculator(MagicMock(), MagicMock(), MagicMock(), MagicMock())

    @patch.object(
        SimpleDeploymentMetricsCalculator,
        "_calculate_workflow",
        return_value={"cost": 1.0, "runtime": 1.0, "carbon": 1.0},
    )
    def test_perform_monte_carlo_simulation(self, mock_calculate_workflow):
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


if __name__ == "__main__":
    unittest.main()
