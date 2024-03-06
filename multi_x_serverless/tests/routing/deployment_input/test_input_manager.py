import unittest
from unittest.mock import MagicMock, Mock

import numpy as np
from requests import patch
from multi_x_serverless.common.models.endpoints import Endpoints
from multi_x_serverless.common.models.remote_client.remote_client import RemoteClient
from multi_x_serverless.routing.models.instance_indexer import InstanceIndexer
from multi_x_serverless.routing.models.region_indexer import RegionIndexer
from multi_x_serverless.routing.deployment_input.input_manager import InputManager
from multi_x_serverless.routing.workflow_config import WorkflowConfig


class TestInputManager(unittest.TestCase):
    def setUp(self):
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
        self.workflow_config.start_hops = "provider1:region1"
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

    def test_get_execution_cost_carbon_runtime_distribution(self):
        self.input_manager._execution_distribution_cache = {"1_2": (np.array([1]), np.array([2]), np.array([3]))}
        self.assertEqual(
            self.input_manager.get_execution_cost_carbon_runtime_distribution(1, 2),
            (np.array([1]), np.array([2]), np.array([3])),
        )

        # For uncached case
        mock_instance_indexer = MagicMock(spec=InstanceIndexer)
        mock_instance_indexer.index_to_value = MagicMock(return_value="node1")
        self.input_manager._instance_indexer = mock_instance_indexer

        mock_region_indexer = MagicMock(spec=RegionIndexer)
        mock_region_indexer.index_to_value = MagicMock(return_value="provider1:region1")
        self.input_manager._region_indexer = mock_region_indexer

        self.input_manager._execution_distribution_cache = {}
        self.input_manager._cost_calculator.calculate_execution_cost_distribution = MagicMock(
            return_value=np.array([10])
        )
        self.input_manager._carbon_calculator.calculate_execution_carbon_distribution = MagicMock(
            return_value=np.array([20])
        )
        self.input_manager._runtime_calculator.calculate_runtime_distribution = MagicMock(return_value=np.array([30]))
        self.assertEqual(
            self.input_manager.get_execution_cost_carbon_runtime_distribution(1, 2),
            (np.array([10]), np.array([20]), np.array([30])),
        )

    def test_get_transmission_cost_carbon_runtime_distribution(self):
        self.input_manager._transmission_distribution_cache = {"1_2_3_4": (np.array([1]), np.array([2]), np.array([3]))}
        self.assertEqual(
            self.input_manager.get_transmission_cost_carbon_runtime_distribution(1, 2, 3, 4),
            (np.array([1]), np.array([2]), np.array([3])),
        )

        # For uncached case
        mock_instance_indexer = MagicMock(spec=InstanceIndexer)
        mock_instance_indexer.index_to_value = MagicMock(return_value="node1")
        self.input_manager._instance_indexer = mock_instance_indexer

        mock_region_indexer = MagicMock(spec=RegionIndexer)
        mock_region_indexer.index_to_value = MagicMock(return_value="provider1:region1")
        self.input_manager._region_indexer = mock_region_indexer

        self.input_manager._transmission_distribution_cache = {}
        self.input_manager._cost_calculator.calculate_transmission_cost_distribution = MagicMock(
            return_value=np.array([10])
        )
        self.input_manager._carbon_calculator.calculate_transmission_carbon_distribution = MagicMock(
            return_value=np.array([20])
        )
        self.input_manager._runtime_calculator.calculate_latency_distribution = MagicMock(return_value=np.array([30]))

        self.assertEqual(
            self.input_manager.get_transmission_cost_carbon_runtime_distribution(1, 2, 3, 4),
            (np.array([10]), np.array([20]), np.array([30])),
        )

    def test_get_execution_cost_carbon_runtime(self):
        self.input_manager.get_execution_cost_carbon_runtime_distribution = MagicMock(
            return_value=([1, 2, 3], [4, 5, 6], [7, 8, 9])
        )
        self.input_manager._consider_probabilistic_invocations = MagicMock(return_value=(1.0, 2.0, 3.0))

        result = self.input_manager.get_execution_cost_carbon_runtime(0, 0, True)

        self.input_manager.get_execution_cost_carbon_runtime_distribution.assert_called_once_with(0, 0)
        self.input_manager._consider_probabilistic_invocations.assert_called_once_with(
            [1, 2, 3], [4, 5, 6], [7, 8, 9], True
        )
        self.assertEqual(result, (1.0, 2.0, 3.0))

    def test_get_transmission_cost_carbon_runtime(self):
        self.input_manager.get_transmission_cost_carbon_runtime_distribution = MagicMock(
            return_value=([1, 2, 3], [4, 5, 6], [7, 8, 9])
        )
        self.input_manager._consider_probabilistic_invocations = MagicMock(return_value=(1.0, 2.0, 3.0))

        result = self.input_manager.get_transmission_cost_carbon_runtime(0, 1, 0, 1, True)

        self.input_manager.get_transmission_cost_carbon_runtime_distribution.assert_called_once_with(0, 1, 0, 1)
        self.input_manager._consider_probabilistic_invocations.assert_called_once_with(
            [1, 2, 3], [4, 5, 6], [7, 8, 9], True
        )
        self.assertEqual(result, (1.0, 2.0, 3.0))

    def test__consider_probabilistic_invocations(self):
        cost_distribution = np.array([1])
        carbon_distribution = np.array([2])
        runtime_distribution = np.array([3])
        self.assertEqual(
            self.input_manager._consider_probabilistic_invocations(
                cost_distribution, carbon_distribution, runtime_distribution, True
            ),
            (1, 2, 3),
        )

    def test_get_all_regions(self):
        self.input_manager._region_viability_loader = Mock()
        self.input_manager._region_viability_loader.get_available_regions.return_value = ["region1", "region2"]
        self.assertEqual(self.input_manager.get_all_regions(), ["region1", "region2"])


if __name__ == "__main__":
    unittest.main()
