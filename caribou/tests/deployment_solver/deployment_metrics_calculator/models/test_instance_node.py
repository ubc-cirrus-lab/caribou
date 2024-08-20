import unittest
from unittest.mock import Mock
from caribou.deployment_solver.deployment_input.input_manager import InputManager
from caribou.deployment_solver.deployment_metrics_calculator.models.instance_node import InstanceNode


class TestInstanceNode(unittest.TestCase):
    def setUp(self):
        self.input_manager = Mock(InputManager)
        self.instance_node = InstanceNode(self.input_manager, instane_id=1)

    def test_get_cumulative_runtime_no_successor(self):
        # Test when there is no specific runtime for the successor
        runtime = self.instance_node.get_cumulative_runtime(successor_instance_index=999)
        self.assertEqual(runtime, 0.0)  # Default current runtime

    def test_get_cumulative_runtime_with_successor(self):
        # Test when there is a specific runtime for the successor
        self.instance_node.cumulative_runtimes["successors"][999] = 10.0
        runtime = self.instance_node.get_cumulative_runtime(successor_instance_index=999)
        self.assertEqual(runtime, 10.0)

    def test_calculate_carbon_cost_runtime_for_virtual_start_instance(self):
        # Test calculation for a virtual start instance
        self.instance_node.actual_instance_id = -1
        self.input_manager.calculate_cost_and_carbon_virtual_start_instance.return_value = {
            "cost": 100.0,
            "execution_carbon": 50.0,
            "transmission_carbon": 10.0,
        }

        metrics = self.instance_node.calculate_carbon_cost_runtime()
        self.assertEqual(metrics["cost"], 100.0)
        self.assertEqual(metrics["runtime"], 0.0)  # Not invoked by default
        self.assertEqual(metrics["execution_carbon"], 50.0)
        self.assertEqual(metrics["transmission_carbon"], 10.0)

    def test_calculate_carbon_cost_runtime_for_actual_instance(self):
        # Test calculation for an actual instance
        self.instance_node.actual_instance_id = 1
        self.instance_node.invoked = True
        self.instance_node.cumulative_runtimes["current"] = 15.0
        self.input_manager.calculate_cost_and_carbon_of_instance.return_value = {
            "cost": 200.0,
            "execution_carbon": 75.0,
            "transmission_carbon": 20.0,
        }

        metrics = self.instance_node.calculate_carbon_cost_runtime()
        self.assertEqual(metrics["cost"], 200.0)
        self.assertEqual(metrics["runtime"], 15.0)  # Runtime since node was invoked
        self.assertEqual(metrics["execution_carbon"], 75.0)
        self.assertEqual(metrics["transmission_carbon"], 20.0)

    def test_is_redirector_when_nominal_instance_id_is_negative(self):
        # Test when the nominal instance ID indicates it's a redirector
        self.instance_node.nominal_instance_id = -1
        self.assertTrue(self.instance_node.is_redirector)

    def test_is_redirector_when_nominal_instance_id_is_positive(self):
        # Test when the nominal instance ID indicates it's not a redirector
        self.instance_node.nominal_instance_id = 1
        self.assertFalse(self.instance_node.is_redirector)

    def test_calculate_carbon_cost_runtime_not_invoked(self):
        # Test that runtime is 0.0 when the node is not invoked
        self.instance_node.invoked = False
        self.input_manager.calculate_cost_and_carbon_of_instance.return_value = {
            "cost": 300.0,
            "execution_carbon": 125.0,
            "transmission_carbon": 30.0,
        }

        metrics = self.instance_node.calculate_carbon_cost_runtime()
        self.assertEqual(metrics["cost"], 300.0)
        self.assertEqual(metrics["runtime"], 0.0)  # Node was not invoked
        self.assertEqual(metrics["execution_carbon"], 125.0)
        self.assertEqual(metrics["transmission_carbon"], 30.0)


if __name__ == "__main__":
    unittest.main()
