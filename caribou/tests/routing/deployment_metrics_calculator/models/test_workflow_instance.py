import unittest
from unittest.mock import MagicMock, patch
from caribou.deployment_solver.deployment_metrics_calculator.models.workflow_instance import WorkflowInstance
from caribou.deployment_solver.deployment_input.input_manager import InputManager
from caribou.deployment_solver.deployment_metrics_calculator.models.instance_edge import InstanceEdge
from caribou.deployment_solver.deployment_metrics_calculator.models.instance_node import InstanceNode
from caribou.deployment_solver.deployment_metrics_calculator.models.simulated_instance_edge import SimulatedInstanceEdge


class TestWorkflowInstance(unittest.TestCase):
    def setUp(self):
        self.input_manager = MagicMock(spec=InputManager)
        self.input_manager.get_start_hop_retrieve_wpd_probability.return_value = 0.0
        self.instance_deployment_regions = [0, 1, 2]
        self.start_hop_instance_index = 0
        self.consider_from_client_latency = True

        self.workflow_instance = WorkflowInstance(
            self.input_manager,
            self.instance_deployment_regions,
            self.start_hop_instance_index,
            self.consider_from_client_latency,
        )

    def test_configure_node_regions(self):
        # No redirector node
        self.input_manager.get_start_hop_retrieve_wpd_probability.return_value = 0.0
        workflow_instance = WorkflowInstance(
            self.input_manager,
            self.instance_deployment_regions,
            self.start_hop_instance_index,
            self.consider_from_client_latency,
        )
        self.workflow_instance._configure_node_regions(self.instance_deployment_regions)
        self.assertEqual(len(workflow_instance._nodes), len(self.instance_deployment_regions) + 1)

        # With redirector node
        self.input_manager.get_start_hop_retrieve_wpd_probability.return_value = 1.0
        workflow_instance = WorkflowInstance(
            self.input_manager,
            self.instance_deployment_regions,
            self.start_hop_instance_index,
            self.consider_from_client_latency,
        )
        self.workflow_instance._configure_node_regions(self.instance_deployment_regions)
        self.assertEqual(len(workflow_instance._nodes), len(self.instance_deployment_regions) + 2)

    def test_add_start_hop(self):
        self.workflow_instance.add_start_hop(1)
        self.assertTrue(self.workflow_instance._nodes[1].invoked)

    def test_add_edge(self):
        self.workflow_instance.add_edge(0, 1, True)
        self.assertIn(0, self.workflow_instance._edges[1])

    def test_add_node(self):
        result = self.workflow_instance.add_node(1)
        self.assertIn(1, self.workflow_instance._nodes)
        self.assertIsInstance(result, bool)

    def test_calculate_overall_cost_runtime_carbon(self):
        result = self.workflow_instance.calculate_overall_cost_runtime_carbon()
        self.assertIsInstance(result, dict)
        self.assertIn("cost", result)
        self.assertIn("runtime", result)
        self.assertIn("carbon", result)

    def test_get_node(self):
        node = self.workflow_instance._get_node(1)
        self.assertIsInstance(node, InstanceNode)

    def test_create_edge(self):
        edge = self.workflow_instance._create_edge(0, 1)
        self.assertIsInstance(edge, InstanceEdge)

    def test_create_simulated_edge(self):
        simulated_edge = self.workflow_instance._create_simulated_edge(0, 1, 2, 3)
        self.assertIsInstance(simulated_edge, SimulatedInstanceEdge)

    def test_manage_data_transfer_dict(self):
        data_transfer_dict = {}
        self.workflow_instance._manage_data_transfer_dict(data_transfer_dict, 1, 100.0)
        self.assertIn(1, data_transfer_dict)
        self.assertEqual(data_transfer_dict[1], 100.0)

    def test_manage_sns_invocation_data_transfer_dict(self):
        sns_data_transfer_dict = {}
        self.workflow_instance._manage_sns_invocation_data_transfer_dict(sns_data_transfer_dict, 1, 100.0)
        self.assertIn(1, sns_data_transfer_dict)
        self.assertEqual(sns_data_transfer_dict[1], [100.0])

    def test_get_predecessor_edges(self):
        edges = self.workflow_instance._get_predecessor_edges(1, False)
        self.assertIsInstance(edges, list)

    def test_retrieved_wpd_at_function(self):
        with patch("random.random", return_value=0.1):
            self.input_manager.get_start_hop_retrieve_wpd_probability.return_value = 0.5
            result = self.workflow_instance._retrieved_wpd_at_function()
            self.assertTrue(result)


if __name__ == "__main__":
    unittest.main()
