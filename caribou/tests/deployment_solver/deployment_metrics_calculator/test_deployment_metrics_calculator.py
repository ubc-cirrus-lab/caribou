import unittest
from unittest.mock import MagicMock, patch

# Assuming these imports are correct and available
from caribou.deployment_solver.deployment_input.input_manager import InputManager
from caribou.deployment_solver.deployment_metrics_calculator.models.workflow_instance import WorkflowInstance
from caribou.deployment_solver.models.dag import DAG
from caribou.deployment_solver.models.instance_indexer import InstanceIndexer
from caribou.deployment_solver.models.region_indexer import RegionIndexer
from caribou.deployment_solver.workflow_config import WorkflowConfig
from caribou.deployment_solver.deployment_metrics_calculator.deployment_metrics_calculator import (
    DeploymentMetricsCalculator,
)


# A subclass to test the abstract class with the abstract method implemented
class DeploymentMetricsCalculatorSubclass(DeploymentMetricsCalculator):
    def _perform_monte_carlo_simulation(self, deployment: list[int]) -> dict[str, float]:
        return {"average_cost": 10.0, "average_runtime": 5.0, "average_carbon": 3.0}


class TestDeploymentMetricsCalculator(unittest.TestCase):
    def setUp(self):
        # Mock the dependencies
        self.workflow_config = MagicMock(spec=WorkflowConfig)

        # Ensure instances are dicts with instance_name keys
        self.workflow_config.instances = {
            0: {"instance_name": "instance_0", "succeeding_instances": [1], "preceding_instances": []},
            1: {"instance_name": "instance_1", "succeeding_instances": [2], "preceding_instances": [0]},
            2: {"instance_name": "instance_2", "succeeding_instances": [], "preceding_instances": [1]},
        }
        self.workflow_config.home_region = "region1"

        self.instance_indexer = MagicMock(spec=InstanceIndexer)

        self.input_manager = MagicMock(spec=InputManager)
        self.input_manager.get_invocation_probability.return_value = 0.5

        # Mock the probability of WPD retrieval
        self.input_manager.get_start_hop_retrieve_wpd_probability.return_value = 0.7  # Fixed probability for testing

        self.region_indexer = MagicMock(spec=RegionIndexer)
        self.region_indexer.get_value_indices.return_value = {"region1": 0}

        # Patch the DAG class to return our mock when initialized
        with patch.object(DAG, "__init__", return_value=None):
            # Create the mock DAG instance
            self.mock_dag = MagicMock(spec=DAG)

            # Configure mock methods of DAG
            self.mock_dag.get_prerequisites_dict.return_value = {0: [], 1: [0], 2: [1]}
            self.mock_dag.get_preceeding_dict.return_value = {0: [1], 1: [2], 2: []}
            self.mock_dag.topological_sort.return_value = [0, 1, 2]

            # Mock the DAG methods in the class
            with patch.object(DAG, "get_prerequisites_dict", self.mock_dag.get_prerequisites_dict), patch.object(
                DAG, "get_preceeding_dict", self.mock_dag.get_preceeding_dict
            ), patch.object(DAG, "topological_sort", self.mock_dag.topological_sort):
                # Initialize the class under test with mocked dependencies
                self.calculator = DeploymentMetricsCalculatorSubclass(
                    workflow_config=self.workflow_config,
                    input_manager=self.input_manager,
                    region_indexer=self.region_indexer,
                    instance_indexer=self.instance_indexer,
                    tail_latency_threshold=1000,
                    record_transmission_execution_carbon=True,
                    consider_from_client_latency=True,
                )

    @patch.object(WorkflowInstance, "add_start_hop")
    @patch.object(WorkflowInstance, "add_node")
    @patch.object(WorkflowInstance, "add_edge")
    @patch.object(WorkflowInstance, "calculate_overall_cost_runtime_carbon")
    @patch.object(
        DeploymentMetricsCalculator, "_is_invoked", return_value=True
    )  # Mock _is_invoked to always return True
    def test_calculate_workflow(
        self, mock_is_invoked, mock_calculate_metrics, mock_add_edge, mock_add_node, mock_add_start_hop
    ):
        # Set up mock return values
        mock_add_node.side_effect = [True, True, False]
        mock_calculate_metrics.return_value = {
            "cost": 100.0,
            "runtime": 20.0,
            "carbon": 10.0,
            "execution_carbon": 6.0,
            "transmission_carbon": 4.0,
        }

        deployment = [0, 1, 2]
        metrics = self.calculator.calculate_workflow(deployment)

        # Verify that the correct calls were made
        mock_add_start_hop.assert_called_once_with(0)
        mock_add_node.assert_any_call(0)
        mock_add_node.assert_any_call(1)
        mock_add_node.assert_any_call(2)

        # Edge calls
        mock_add_edge.assert_any_call(0, 1, True)
        mock_add_edge.assert_any_call(1, 2, True)

        # Check that the metrics are correctly returned
        self.assertEqual(metrics["cost"], 100.0)
        self.assertEqual(metrics["runtime"], 20.0)
        self.assertEqual(metrics["carbon"], 10.0)

    def test_is_invoked(self):
        self.input_manager.get_invocation_probability.return_value = 1.0
        self.assertTrue(self.calculator._is_invoked(0, 1))

        self.input_manager.get_invocation_probability.return_value = 0.0
        self.assertFalse(self.calculator._is_invoked(0, 1))

    @patch.object(DeploymentMetricsCalculatorSubclass, "_perform_monte_carlo_simulation")
    def test_calculate_deployment_metrics(self, mock_monte_carlo):
        # Define the mock results
        mock_monte_carlo.return_value = {
            "average_cost": 2.0,
            "average_runtime": 2.0,
            "average_carbon": 2.0,
            "tail_cost": 1.0,
            "tail_runtime": 1.0,
            "tail_carbon": 1.0,
        }

        # Call the method with a test deployment
        deployment = [0, 1, 2]
        self.calculator._topological_order = [i for i in range(len(deployment))]
        metrics = self.calculator.calculate_deployment_metrics(deployment)

        # Verify the results
        self.assertEqual(metrics["average_cost"], 2.0)
        self.assertEqual(metrics["average_runtime"], 2.0)
        self.assertEqual(metrics["average_carbon"], 2.0)
        self.assertEqual(metrics["tail_cost"], 1.0)
        self.assertEqual(metrics["tail_runtime"], 1.0)
        self.assertEqual(metrics["tail_carbon"], 1.0)

        # Verify that the mock methods were called with the correct arguments
        mock_monte_carlo.assert_called_once_with(deployment)


if __name__ == "__main__":
    unittest.main()
