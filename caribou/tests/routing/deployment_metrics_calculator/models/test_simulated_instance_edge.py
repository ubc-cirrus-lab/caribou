import unittest
from unittest.mock import Mock
from caribou.deployment_solver.deployment_input.input_manager import InputManager
from caribou.deployment_solver.deployment_metrics_calculator.models.instance_node import InstanceNode
from caribou.deployment_solver.deployment_metrics_calculator.models.simulated_instance_edge import SimulatedInstanceEdge


class TestSimulatedInstanceEdge(unittest.TestCase):
    def setUp(self):
        self.input_manager = Mock(InputManager)
        self.from_instance_node = Mock(InstanceNode)
        self.to_instance_node = Mock(InstanceNode)
        self.simulated_edge = SimulatedInstanceEdge(
            input_manager=self.input_manager,
            from_instance_node=self.from_instance_node,
            to_instance_node=self.to_instance_node,
            uninvoked_instance_id=5,
            simulated_sync_predecessor_id=7,
        )

    def test_get_simulated_transmission_information_with_invoked_node(self):
        # Setup the mocked node as invoked
        self.from_instance_node.invoked = True
        self.from_instance_node.actual_instance_id = 1
        self.from_instance_node.region_id = 10
        self.from_instance_node.get_cumulative_runtime.return_value = 15.0
        self.to_instance_node.actual_instance_id = 2
        self.to_instance_node.region_id = 20

        self.input_manager.get_simulated_transmission_info.return_value = {"latency": 0.1, "cost": 1.5, "carbon": 0.05}

        result = self.simulated_edge.get_simulated_transmission_information()

        self.assertIsNotNone(result)
        self.assertEqual(result["latency"], 0.1)
        self.assertEqual(result["cost"], 1.5)
        self.assertEqual(result["carbon"], 0.05)

        self.input_manager.get_simulated_transmission_info.assert_called_once_with(1, 5, 7, 2, 10, 20, 15.0)

    def test_get_simulated_transmission_information_with_uninvoked_node(self):
        # Setup the mocked node as not invoked
        self.from_instance_node.invoked = False

        result = self.simulated_edge.get_simulated_transmission_information()

        self.assertIsNone(result)
        self.input_manager.get_simulated_transmission_info.assert_not_called()

    def test_get_simulated_transmission_information_with_runtime_from_parent_node(self):
        # Setup the mocked node with cumulative runtime from parent node
        self.from_instance_node.invoked = True
        self.from_instance_node.actual_instance_id = 1
        self.from_instance_node.region_id = 10
        self.from_instance_node.get_cumulative_runtime.return_value = 30.0
        self.to_instance_node.actual_instance_id = 2
        self.to_instance_node.region_id = 20

        self.input_manager.get_simulated_transmission_info.return_value = {"latency": 0.2, "cost": 2.0, "carbon": 0.08}

        result = self.simulated_edge.get_simulated_transmission_information()

        self.assertIsNotNone(result)
        self.assertEqual(result["latency"], 0.2)
        self.assertEqual(result["cost"], 2.0)
        self.assertEqual(result["carbon"], 0.08)

        self.input_manager.get_simulated_transmission_info.assert_called_once_with(1, 5, 7, 2, 10, 20, 30.0)


if __name__ == "__main__":
    unittest.main()
