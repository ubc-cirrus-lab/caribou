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

    def test_get_execution_cost_carbon_latency(self):
        self.input_manager._instance_indexer = MagicMock(spec=InstanceIndexer)
        self.input_manager._region_indexer = MagicMock(spec=RegionIndexer)

        self.input_manager._runtime_calculator = MagicMock(spec=RuntimeCalculator)
        self.input_manager._runtime_calculator.calculate_runtime_distribution.return_value = np.array([3])

        self.input_manager._cost_calculator = MagicMock(spec=CostCalculator)
        self.input_manager._carbon_calculator = MagicMock(spec=CarbonCalculator)

        self.input_manager._execution_latency_distribution_cache = {}

        self.input_manager.get_execution_cost_carbon_latency(1, 2)  # run the function

        # Asset calls
        self.input_manager._instance_indexer.index_to_value.assert_called_once()
        self.input_manager._region_indexer.index_to_value.assert_called_once()

        self.input_manager._runtime_calculator.calculate_runtime_distribution.assert_called_once()
        self.input_manager._cost_calculator.calculate_execution_cost.assert_called_once()
        self.input_manager._carbon_calculator.calculate_execution_carbon.assert_called_once()

    def test_get_transmission_cost_carbon_latency(self):
        self.input_manager._instance_indexer = MagicMock(spec=InstanceIndexer)
        self.input_manager._region_indexer = MagicMock(spec=RegionIndexer)

        self.input_manager._runtime_calculator = MagicMock(spec=RuntimeCalculator)
        self.input_manager._runtime_calculator.get_transmission_size_distribution.return_value = np.array([3])
        self.input_manager._runtime_calculator.get_transmission_latency_distribution.return_value = np.array([3])

        self.input_manager._cost_calculator = MagicMock(spec=CostCalculator)
        self.input_manager._carbon_calculator = MagicMock(spec=CarbonCalculator)

        self.input_manager._execution_latency_distribution_cache = {}

        self.input_manager.get_transmission_cost_carbon_latency(1, 2, 3, 4)  # run the function

        # Asset calls
        self.input_manager._instance_indexer.index_to_value.assert_has_calls([call(1), call(2)])
        self.input_manager._region_indexer.index_to_value.assert_has_calls([call(3), call(4)])

        self.input_manager._runtime_calculator.get_transmission_size_distribution.assert_called_once()
        self.input_manager._runtime_calculator.get_transmission_latency_distribution.assert_called_once()

        self.input_manager._cost_calculator.calculate_transmission_cost.assert_called_once()
        self.input_manager._carbon_calculator.calculate_transmission_carbon.assert_called_once()

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
