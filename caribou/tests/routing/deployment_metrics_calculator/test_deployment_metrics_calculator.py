import unittest
from unittest.mock import MagicMock, Mock, patch
from caribou.routing.deployment_metrics_calculator.deployment_metrics_calculator import (
    DeploymentMetricsCalculator,
)


class DeploymentMetricsCalculatorSubclass(DeploymentMetricsCalculator):
    def _perform_monte_carlo_simulation(self, deployment: list[int]) -> dict[str, float]:
        pass


class TestDeploymentMetricsCalculator(unittest.TestCase):
    def setUp(self):
        self.workflow_config = MagicMock()
        self.instance_indexer = MagicMock()
        self.input_manager = MagicMock()
        self.region_indexer = MagicMock()

        # Say this is the value of deploying an instance at a region (row = from, col = to) # row = instance_index, column = region_index
        # For simplicity, we just say that cost just this value, co2 is this value * 2, and rt is also just this value
        self.execution_matrix = [
            [5.0, 6.0],
            [7.0, 8.0],
            [9.0, 10.0],
            [11.0, 12.0],
            [13.0, 14.0],
            [15.0, 16.0],
        ]

        # Simplify it to 1 array as it might be easier to understand (So say cos/co2/rt have same base values)
        # previous_instance_index, current_instance_index, from_region_index, to_region_index
        # Here we only consider the from to regions
        # But we just use a factor and then simply use from to of regions here
        # Say this is the value of from a region to a region (row = from, col = to)
        # For simplicity, we just say that cost just this value, co2 is this value * 2, and rt is also just this value
        self.transmission_matrix = [
            [0, 1],
            [2, 0],
        ]

        # Mock input manager
        self.input_manager.get_execution_cost_carbon_latency.side_effect = (
            lambda current_instance_index, to_region_index: (
                (self.execution_matrix[current_instance_index][to_region_index]),
                (self.execution_matrix[current_instance_index][to_region_index]),
                (self.execution_matrix[current_instance_index][to_region_index]),
            )
        )

        self.input_manager.get_transmission_cost_carbon_latency.side_effect = (
            lambda previous_instance_index, current_instance_index, from_region_index, to_region_index: (
                (self.transmission_matrix[from_region_index][to_region_index]),
                (self.transmission_matrix[from_region_index][to_region_index]),
                (self.transmission_matrix[from_region_index][to_region_index]),
            )
            if previous_instance_index != -1
            else (0, 0, 0)  # Do not consider start hop
        )

        self.is_invoked_matrix = [
            [True, True],
            [True, True],
        ]
        self.is_invoked_replacement = lambda current_instance_index, to_region_index: (
            self.is_invoked_matrix[current_instance_index][to_region_index]
        )

        self.calculator = DeploymentMetricsCalculatorSubclass(
            self.workflow_config, self.input_manager, self.region_indexer, self.instance_indexer
        )

        self.calculator._home_region_index = 0

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

    def test_is_invoked(self):
        self.input_manager.get_invocation_probability.return_value = 1.0
        self.assertTrue(self.calculator._is_invoked(0, 1))

        self.input_manager.get_invocation_probability.return_value = 0.0
        self.assertFalse(self.calculator._is_invoked(0, 1))

    @patch.object(DeploymentMetricsCalculator, "_is_invoked")
    def test_calculate_workflow_2_nodes(self, mock_is_invoked):
        # Represent a case where the workflow has only 2 instances, deployed in region 0 and 1
        deployment = [0, 1]
        self.calculator._prerequisites_dictionary = {
            0: [],
            1: [0],
        }
        self.calculator._successor_dictionary = {
            0: [1],
            1: [],
        }
        self.is_invoked_matrix = {
            0: {
                1: False,
            },
        }
        mock_is_invoked.side_effect = self.is_invoked_replacement

        pc_metrics = self.calculator._calculate_workflow(deployment)
        self.assertEqual(pc_metrics, {"cost": 5.0, "runtime": 5.0, "carbon": 5.0})

    @patch.object(DeploymentMetricsCalculator, "_is_invoked")
    def test_calculate_workflow_3_nodes(self, mock_is_invoked):
        # Represent a case where the workflow has only 3 instances, where there are 2 childs
        deployment = [0, 1, 1]
        self.calculator._prerequisites_dictionary = {
            0: [],
            1: [0],
            2: [0],
        }
        self.calculator._successor_dictionary = {
            0: [1, 2],
            1: [],
            2: [],
        }
        self.is_invoked_matrix = {
            0: {
                1: True,
                2: False,
            },
        }

        mock_is_invoked.side_effect = self.is_invoked_replacement

        pc_metrics = self.calculator._calculate_workflow(deployment)
        self.assertEqual(pc_metrics, {"cost": 14.0, "runtime": 14.0, "carbon": 14.0})

    @patch.object(DeploymentMetricsCalculator, "_is_invoked")
    def test_calculate_workflow_5_nodes_base(self, mock_is_invoked):
        # Represent a case where the workflow has 5 instances 1 root with 3 childs, 2 of the childs join to 1 child
        deployment = [0, 0, 0, 0, 0]
        self.calculator._prerequisites_dictionary = {0: [], 1: [0], 2: [0], 3: [0], 4: [1, 2]}
        self.calculator._successor_dictionary = {
            0: [1, 2, 3],
            1: [4],
            2: [4],
            3: [],
            4: [],
        }
        self.is_invoked_matrix = {
            0: {
                1: True,
                2: True,
                3: True,
            },
            1: {
                4: True,
            },
            2: {
                4: True,
            },
        }
        mock_is_invoked.side_effect = self.is_invoked_replacement

        pc_metrics = self.calculator._calculate_workflow(deployment)
        self.assertEqual(pc_metrics, {"cost": 45.0, "runtime": 27.0, "carbon": 45.0})

    @patch.object(DeploymentMetricsCalculator, "_is_invoked")
    def test_calculate_workflow_5_nodes_pcs(self, mock_is_invoked):
        # Represent a case where the workflow has 5 instances 1 root with 3 childs, 2 of the childs join to 1 child
        deployment = [0, 0, 0, 0, 0]
        self.calculator._prerequisites_dictionary = {0: [], 1: [0], 2: [0], 3: [0], 4: [1, 2]}
        self.calculator._successor_dictionary = {
            0: [1, 2, 3],
            1: [4],
            2: [4],
            3: [],
            4: [],
        }
        mock_is_invoked.side_effect = self.is_invoked_replacement
        self.transmission_matrix = [[1]]
        # Case 1 - ALL on
        self.is_invoked_matrix = {
            0: {
                1: True,
                2: True,
                3: True,
            },
            1: {
                4: True,
            },
            2: {
                4: True,
            },
        }
        pc_metrics = self.calculator._calculate_workflow(deployment)
        self.assertEqual(pc_metrics, {"cost": 50.0, "runtime": 29.0, "carbon": 50.0})

        # Case 2
        self.is_invoked_matrix = {
            0: {
                1: True,
                2: True,
                3: True,
            },
            1: {
                4: False,
            },
            2: {
                4: True,
            },
        }
        pc_metrics = self.calculator._calculate_workflow(deployment)
        self.assertEqual(pc_metrics, {"cost": 49.0, "runtime": 29.0, "carbon": 49.0})

        # Case 3
        self.is_invoked_matrix = {
            0: {
                1: True,
                2: True,
                3: True,
            },
            1: {
                4: False,
            },
            2: {
                4: False,
            },
        }
        pc_metrics = self.calculator._calculate_workflow(deployment)
        self.assertEqual(pc_metrics, {"cost": 35.0, "runtime": 17.0, "carbon": 35.0})

        # Case 4
        self.is_invoked_matrix = {
            0: {
                1: True,
                2: True,
                3: False,
            },
            1: {
                4: False,
            },
            2: {
                4: False,
            },
        }
        pc_metrics = self.calculator._calculate_workflow(deployment)
        self.assertEqual(pc_metrics, {"cost": 23.0, "runtime": 15.0, "carbon": 23.0})

        # Case 5
        self.is_invoked_matrix = {
            0: {
                1: True,
                2: False,
                3: True,
            },
            1: {
                4: True,
            },
            2: {
                4: False,
            },
        }
        pc_metrics = self.calculator._calculate_workflow(deployment)
        self.assertEqual(pc_metrics, {"cost": 39.0, "runtime": 27.0, "carbon": 39.0})

        # Case 6
        self.is_invoked_matrix = {
            0: {
                1: False,
                2: False,
                3: False,
            },
        }
        pc_metrics = self.calculator._calculate_workflow(deployment)
        self.assertEqual(pc_metrics, {"cost": 5.0, "runtime": 5.0, "carbon": 5.0})


if __name__ == "__main__":
    unittest.main()
