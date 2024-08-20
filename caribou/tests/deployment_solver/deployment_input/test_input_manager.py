import unittest
from unittest.mock import MagicMock, Mock, patch, call

import numpy as np
from caribou.common.models.remote_client.remote_client import RemoteClient
from caribou.deployment_solver.deployment_input.components.calculators.carbon_calculator import CarbonCalculator
from caribou.deployment_solver.deployment_input.components.calculators.cost_calculator import CostCalculator
from caribou.deployment_solver.deployment_input.components.calculators.runtime_calculator import RuntimeCalculator
from caribou.deployment_solver.deployment_input.components.loaders.carbon_loader import CarbonLoader
from caribou.deployment_solver.deployment_input.components.loaders.datacenter_loader import DatacenterLoader
from caribou.deployment_solver.deployment_input.components.loaders.performance_loader import PerformanceLoader
from caribou.deployment_solver.models.instance_indexer import InstanceIndexer
from caribou.deployment_solver.models.region_indexer import RegionIndexer
from caribou.deployment_solver.deployment_input.input_manager import InputManager
from caribou.deployment_solver.workflow_config import WorkflowConfig


class TestInputManager(unittest.TestCase):
    @patch(InputManager.__module__ + ".RegionViabilityLoader")
    @patch(InputManager.__module__ + ".Endpoints")
    def setUp(self, mock_endpoints, mock_region_viability_loader):
        self.workflow_config = Mock(spec=WorkflowConfig)
        self.workflow_config.instances = {
            "node1": {
                "instance_name": "node1",
                "succeeding_instances": ["node2"],
                "preceding_instances": [],
                "regions_and_providers": {
                    "allowed_regions": None,
                    "disallowed_regions": None,
                    "providers": {"provider1": None},
                },
            },
            "node2": {
                "instance_name": "node2",
                "succeeding_instances": [],
                "preceding_instances": ["node1"],
                "regions_and_providers": {
                    "allowed_regions": None,
                    "disallowed_regions": None,
                    "providers": {"provider1": None},
                },
            },
        }
        self.workflow_config.regions_and_providers = {
            "allowed_regions": None,
            "disallowed_regions": None,
            "providers": {"provider1": None, "provider2": None, "provider3": None},
        }
        self.workflow_config.workflow_id = "workflow_id"
        self.workflow_config.home_region = "provider1:region1"
        mock_endpoints.get_data_collector_client = MagicMock(return_value=RemoteClient)
        mock_region_viability_loader.get_available_regions = MagicMock(return_value=["region1", "region2"])
        self.input_manager = InputManager(self.workflow_config)

    def test_init(self):
        self.assertEqual(self.input_manager._workflow_config, self.workflow_config)

    def test_setup(self):
        self.input_manager._workflow_loader.setup = MagicMock()
        self.input_manager._datacenter_loader.setup = MagicMock()
        self.input_manager._performance_loader.setup = MagicMock()
        self.input_manager._carbon_loader.setup = MagicMock()

        region_indexer = MagicMock(spec=RegionIndexer)
        region_indexer.get_value_indices = MagicMock(return_value={"provider1:region1": 0, "provider1:region2": 1})
        instance_indexer = MagicMock(spec=InstanceIndexer)
        instance_indexer.get_instance_index = MagicMock(return_value={"node1": 0, "node2": 1})
        self.input_manager.setup(region_indexer, instance_indexer)

        self.input_manager._workflow_loader.setup.assert_called_once()
        self.input_manager._datacenter_loader.setup.assert_called_once()
        self.input_manager._performance_loader.setup.assert_called_once()
        self.input_manager._carbon_loader.setup.assert_called_once()

    def test_invalid_workflow_id_in_setup(self):
        self.input_manager._workflow_config.workflow_id = None

        with self.assertRaises(ValueError) as context:
            self.input_manager.setup(MagicMock(spec=RegionIndexer), MagicMock(spec=InstanceIndexer))
        self.assertEqual(str(context.exception), "Workflow ID is not set in the config")

    def test_tail_latency_threshold_validation(self):
        with self.assertRaises(ValueError) as context:
            InputManager(self.workflow_config, tail_latency_threshold=49)
        self.assertEqual(str(context.exception), "Tail threshold must be between 50 and 100")

        with self.assertRaises(ValueError) as context:
            InputManager(self.workflow_config, tail_latency_threshold=101)
        self.assertEqual(str(context.exception), "Tail threshold must be between 50 and 100")

    def test_cache_clearing_in_alter_carbon_setting(self):
        self.input_manager._carbon_calculator.alter_carbon_setting = MagicMock()
        self.input_manager._execution_latency_distribution_cache = {"key": [0.5]}
        self.input_manager._invocation_probability_cache = {"key": 0.5}

        self.input_manager.alter_carbon_setting("1")

        self.assertEqual(self.input_manager._execution_latency_distribution_cache, {})
        self.assertEqual(self.input_manager._invocation_probability_cache, {})

    def test_get_transmission_info(self):
        self.input_manager._runtime_calculator = MagicMock()
        self.input_manager._instance_indexer = MagicMock()
        self.input_manager._region_indexer = MagicMock()

        self.input_manager._instance_indexer.index_to_value.side_effect = ["node1", "node2"]
        self.input_manager._region_indexer.index_to_value.side_effect = ["region1", "region2"]

        self.input_manager._runtime_calculator.calculate_transmission_size_and_latency.return_value = (10.0, 1.0)

        result = self.input_manager.get_transmission_info(
            from_instance_index=0,
            from_region_index=0,
            to_instance_index=1,
            to_region_index=1,
            cumulative_runtime=5.0,
            to_instance_is_sync_node=False,
            consider_from_client_latency=True,
        )

        self.assertEqual(result["starting_runtime"], 5.0)
        self.assertEqual(result["cumulative_runtime"], 6.0)
        self.assertEqual(result["sns_data_transfer_size"], 10.0)
        self.assertIsNone(result["sync_info"])

    def test_calculate_cost_and_carbon_of_instance(self):
        self.input_manager._cost_calculator = MagicMock()
        self.input_manager._carbon_calculator = MagicMock()
        self.input_manager._instance_indexer = MagicMock()
        self.input_manager._region_indexer = MagicMock()

        # Assume there are multiple regions to convert
        self.input_manager._instance_indexer.index_to_value.side_effect = ["node1"]
        self.input_manager._region_indexer.index_to_value.side_effect = lambda idx: f"region{idx}"

        self.input_manager._cost_calculator.calculate_instance_cost.return_value = 100.0
        self.input_manager._carbon_calculator.calculate_instance_carbon.return_value = (10.0, 20.0)

        result = self.input_manager.calculate_cost_and_carbon_of_instance(
            execution_time=10.0,
            instance_index=0,
            region_index=0,
            data_input_sizes={1: 10.0},
            data_output_sizes={2: 20.0},
            sns_data_call_and_output_sizes={3: [5.0]},
            data_transfer_during_execution=15.0,
            dynamodb_read_capacity=5.0,
            dynamodb_write_capacity=10.0,
            is_invoked=True,
            is_redirector=False,
        )

        self.assertEqual(result["cost"], 100.0)
        self.assertEqual(result["execution_carbon"], 10.0)
        self.assertEqual(result["transmission_carbon"], 20.0)

    def test_missing_home_region_in_setup(self):
        region_indexer = MagicMock(spec=RegionIndexer)
        region_indexer.get_value_indices = MagicMock(return_value={"provider2:region2": 0})

        instance_indexer = MagicMock(spec=InstanceIndexer)
        instance_indexer.get_instance_index = MagicMock(return_value={"node1": 0})

        self.input_manager._workflow_loader = MagicMock()
        self.input_manager._workflow_loader.get_home_region.return_value = "provider1:region1"

        with self.assertRaises(ValueError) as context:
            self.input_manager.setup(region_indexer, instance_indexer)

        self.assertEqual(
            str(context.exception),
            "Home region of the workflow is not in the requested regions! This should NEVER happen!",
        )

    def test_alter_carbon_setting(self):
        self.input_manager._carbon_calculator.alter_carbon_setting = MagicMock()
        self.input_manager.alter_carbon_setting("1")
        self.input_manager._carbon_calculator.alter_carbon_setting.assert_called_once_with("1")

    def test_get_invocation_probability(self):
        mock_instance_indexer = MagicMock(spec=InstanceIndexer)
        mock_instance_indexer.index_to_value = MagicMock(return_value="node1")
        self.input_manager._instance_indexer = mock_instance_indexer

        # For cached case
        self.input_manager._invocation_probability_cache = {"1_2": 0.4}
        self.assertEqual(self.input_manager.get_invocation_probability(1, 2), 0.4)

        # For uncached case
        self.input_manager._invocation_probability_cache = {}
        self.input_manager._workflow_loader.get_invocation_probability = MagicMock(return_value=0.5)
        self.assertEqual(self.input_manager.get_invocation_probability(1, 2), 0.5)
        self.assertEqual(self.input_manager._invocation_probability_cache, {"1_2": 0.5})

        # Assert called indexers
        self.input_manager._instance_indexer.index_to_value.assert_any_call(1)
        self.input_manager._instance_indexer.index_to_value.assert_any_call(2)

    def test_get_all_regions(self):
        self.input_manager._region_viability_loader = Mock()
        self.input_manager._region_viability_loader.get_available_regions.return_value = ["region1", "region2"]
        self.assertEqual(self.input_manager.get_all_regions(), ["region1", "region2"])

    def test_get_state(self):
        self.input_manager._region_viability_loader = Mock()
        self.input_manager._region_viability_loader.get_available_regions.return_value = ["region1", "region2"]
        self.input_manager._carbon_loader = MagicMock()
        self.input_manager._workflow_loader = MagicMock()
        self.input_manager._carbon_calculator = MagicMock()
        state = self.input_manager.__getstate__()
        self.assertEqual(state.get("_region_viability_loader"), ["region1", "region2"])

    @patch(InputManager.__module__ + ".RegionViabilityLoader")
    @patch(InputManager.__module__ + ".Endpoints")
    @patch(InputManager.__module__ + ".DatacenterLoader")
    @patch(InputManager.__module__ + ".PerformanceLoader")
    @patch(InputManager.__module__ + ".CarbonLoader")
    def test_set_state(
        self,
        mock_endpoints,
        mock_region_viability_loader,
        mock_datacenter_loader,
        mock_performance_loader,
        mock_carbon_loader,
    ):
        mock_datacenter_loader_instance = mock_datacenter_loader.return_value
        mock_datacenter_loader_instance.setup.return_value = None
        self.input_manager._region_indexer = MagicMock(spec=RegionIndexer)
        self.input_manager._region_indexer.get_value_indices = MagicMock(
            return_value={"provider1:region1": 0, "provider1:region2": 1}
        )
        state = MagicMock()
        state.__getitem__.return_value = {}
        self.input_manager.__setstate__(state)
        mock_datacenter_loader_instance.setup.assert_called_once()


if __name__ == "__main__":
    unittest.main()
