import unittest
from unittest.mock import MagicMock, patch, Mock

from caribou.deployment_solver.deployment_input.input_manager import InputManager
from caribou.deployment_solver.deployment_metrics_calculator.simple_deployment_metrics_calculator import (
    SimpleDeploymentMetricsCalculator,
)
from caribou.deployment_solver.models.instance_indexer import InstanceIndexer
from caribou.deployment_solver.models.region_indexer import RegionIndexer
from caribou.deployment_solver.workflow_config import WorkflowConfig


class TestSimpleDeploymentMetricsCalculator(unittest.TestCase):
    @patch.object(
        SimpleDeploymentMetricsCalculator,
        "calculate_workflow",
        return_value={"cost": 1.0, "runtime": 1.0, "carbon": 1.0},
    )
    @patch.object(
        WorkflowConfig,
        "__init__",
        return_value=None
    )
    @patch.object(
        InputManager,
        "__init__",
        return_value=None
    )
    @patch.object(
        RegionIndexer,
        "__init__",
        return_value=None
    )
    @patch.object(
        InstanceIndexer,
        "__init__",
        return_value=None
    )
    def test_perform_monte_carlo_simulation(
            self,
            mock_calculate_workflow,
            mock_workflow_config,
            mock_input_manager,
            mock_region_index,
            mock_instance_indexer
    ):
        self.calculator = SimpleDeploymentMetricsCalculator(
            WorkflowConfig(
                MagicMock()
            ), InputManager(
                MagicMock()
            ), RegionIndexer(
                MagicMock()
            ), InstanceIndexer(
                MagicMock()
            ),
            n_processes=4
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


if __name__ == "__main__":
    unittest.main()
