import unittest
from unittest.mock import MagicMock, Mock
from multi_x_serverless.common.models.endpoints import Endpoints
from multi_x_serverless.common.models.remote_client.remote_client import RemoteClient
from multi_x_serverless.routing.models.dag import DAG
from multi_x_serverless.routing.models.region_indexer import RegionIndexer
from multi_x_serverless.routing.solver.input.input_manager import InputManager
from multi_x_serverless.routing.workflow_config import WorkflowConfig


class TestInputManager(unittest.TestCase):
    def setUp(self):
        self.workflow_config = Mock(spec=WorkflowConfig)
        self.workflow_config.instances = [
            {
                "instance_name": "node1",
                "succeeding_instances": ["node2"],
                "preceding_instances": [],
                "regions_and_providers": {
                    "allowed_regions": None,
                    "disallowed_regions": None,
                    "providers": {"provider1": None},
                },
            },
            {
                "instance_name": "node2",
                "succeeding_instances": [],
                "preceding_instances": ["node1"],
                "regions_and_providers": {
                    "allowed_regions": None,
                    "disallowed_regions": None,
                    "providers": {"provider1": None},
                },
            },
        ]
        self.workflow_config.regions_and_providers = {
            "allowed_regions": None,
            "disallowed_regions": None,
            "providers": {"provider1": None, "provider2": None, "provider3": None},
        }
        self.workflow_config.workflow_id = "workflow_id"
        self.workflow_config.start_hops = "provider1:region1"
        self.input_manager = InputManager(self.workflow_config, False)

    def test_setup(self):
        self.input_manager._workflow_loader.setup = MagicMock()
        self.input_manager._workflow_loader.get_all_favorite_regions = MagicMock(
            return_value=["provider1:region1", "provider1:region2"]
        )
        self.input_manager._datacenter_loader.setup = MagicMock()
        self.input_manager._performance_loader.setup = MagicMock()
        self.input_manager._carbon_loader.setup = MagicMock()

        region_indexer = MagicMock(spec=RegionIndexer)
        region_indexer.get_value_indices = MagicMock(return_value={"provider1:region1": 0, "provider1:region2": 1})
        instance_indexer = MagicMock(spec=DAG)
        instance_indexer.get_instance_index = MagicMock(return_value={"node1": 0, "node2": 1})
        self.input_manager.setup(region_indexer, instance_indexer)

        self.input_manager._workflow_loader.setup.assert_called_once()
        self.input_manager._datacenter_loader.setup.assert_called_once()
        self.input_manager._performance_loader.setup.assert_called_once()
        self.input_manager._carbon_loader.setup.assert_called_once()

    def test_get_execution_cost_carbon_runtime(self):
        self.input_manager._instance_indexer = MagicMock(spec=DAG)
        self.input_manager._region_indexer = MagicMock(spec=RegionIndexer)

        self.input_manager._cost_calculator.calculate_execution_cost = MagicMock(return_value=10.0)
        self.input_manager._carbon_calculator.calculate_execution_carbon = MagicMock(return_value=20.0)
        self.input_manager._runtime_calculator.calculate_runtime = MagicMock(return_value=30.0)

        result = self.input_manager.get_execution_cost_carbon_runtime(0, 0)
        self.assertEqual(result, [10.0, 20.0, 30.0])

    def test_get_transmission_cost_carbon_runtime(self):
        self.input_manager._instance_indexer = MagicMock(spec=DAG)
        self.input_manager._region_indexer = MagicMock(spec=RegionIndexer)

        self.input_manager._cost_calculator.calculate_transmission_cost = Mock(return_value=10.0)
        self.input_manager._carbon_calculator.calculate_transmission_carbon = Mock(return_value=20.0)
        self.input_manager._runtime_calculator.calculate_latency = Mock(return_value=30.0)

        result = self.input_manager.get_transmission_cost_carbon_runtime(0, 1, 0, 1)
        self.assertEqual(result, [10.0, 20.0, 30.0])

    def test_get_all_regions(self):
        self.input_manager._region_viability_loader.get_available_regions = Mock(return_value=["region1", "region2"])
        result = self.input_manager.get_all_regions()
        self.assertEqual(result, ["region1", "region2"])


if __name__ == "__main__":
    unittest.main()
