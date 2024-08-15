import unittest
from unittest.mock import Mock, patch
from caribou.deployment_solver.deployment_input.input_manager import InputManager
from caribou.deployment_solver.deployment_metrics_calculator.models.instance_node import InstanceNode
from caribou.deployment_solver.deployment_metrics_calculator.models.instance_edge import InstanceEdge


class TestInstanceEdge(unittest.TestCase):
    def setUp(self):
        self.mock_input_manager = Mock(spec=InputManager)
        self.mock_from_instance_node = Mock(spec=InstanceNode)
        self.mock_to_instance_node = Mock(spec=InstanceNode)

        # Setting up necessary attributes on mock objects
        self.mock_from_instance_node.actual_instance_id = 1
        self.mock_from_instance_node.region_id = "us-east-1"
        self.mock_from_instance_node.invoked = True

        self.mock_to_instance_node.actual_instance_id = 2
        self.mock_to_instance_node.region_id = "us-west-2"

        self.instance_edge = InstanceEdge(
            input_manager=self.mock_input_manager,
            from_instance_node=self.mock_from_instance_node,
            to_instance_node=self.mock_to_instance_node,
        )

    def test_instance_edge_initialization(self):
        self.assertEqual(self.instance_edge._input_manager, self.mock_input_manager)
        self.assertEqual(self.instance_edge.from_instance_node, self.mock_from_instance_node)
        self.assertEqual(self.instance_edge.to_instance_node, self.mock_to_instance_node)
        self.assertFalse(self.instance_edge.conditionally_invoked)

    def test_get_transmission_information_edge_not_real(self):
        # Simulate the from_instance_node not being invoked
        self.mock_from_instance_node.invoked = False

        # Call the method and check the result
        result = self.instance_edge.get_transmission_information(
            successor_is_sync_node=False, consider_from_client_latency=False
        )
        self.assertIsNone(result)

    def test_get_transmission_information_conditionally_invoked(self):
        # Simulate the from_instance_node being invoked and the edge being conditionally invoked
        self.mock_from_instance_node.invoked = True
        self.instance_edge.conditionally_invoked = True

        self.mock_from_instance_node.get_cumulative_runtime.return_value = 10.0

        # Expected return value from the mock InputManager
        expected_transmission_info = {"latency": 1.0, "bandwidth": 100.0}
        self.mock_input_manager.get_transmission_info.return_value = expected_transmission_info

        # Call the method and check the result
        result = self.instance_edge.get_transmission_information(
            successor_is_sync_node=True, consider_from_client_latency=True
        )

        self.mock_input_manager.get_transmission_info.assert_called_once_with(
            self.mock_from_instance_node.actual_instance_id,
            self.mock_from_instance_node.region_id,
            self.mock_to_instance_node.actual_instance_id,
            self.mock_to_instance_node.region_id,
            10.0,
            True,
            True,
        )
        self.assertEqual(result, expected_transmission_info)

    def test_get_transmission_information_not_conditionally_invoked(self):
        # Simulate the from_instance_node being invoked and the edge not being conditionally invoked
        self.mock_from_instance_node.invoked = True
        self.instance_edge.conditionally_invoked = False

        # Expected return value from the mock InputManager
        expected_non_execution_info = {"status": "not_executed"}
        self.mock_input_manager.get_non_execution_info.return_value = expected_non_execution_info

        # Call the method and check the result
        result = self.instance_edge.get_transmission_information(
            successor_is_sync_node=False, consider_from_client_latency=False
        )

        self.mock_input_manager.get_non_execution_info.assert_called_once_with(
            self.mock_from_instance_node.actual_instance_id, self.mock_to_instance_node.actual_instance_id
        )
        self.assertEqual(result, expected_non_execution_info)


if __name__ == "__main__":
    unittest.main()
