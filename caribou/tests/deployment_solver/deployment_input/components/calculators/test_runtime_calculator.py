import unittest
from unittest.mock import MagicMock, patch
from caribou.deployment_solver.deployment_input.components.calculator import InputCalculator
from caribou.deployment_solver.deployment_input.components.loaders.performance_loader import PerformanceLoader
from caribou.deployment_solver.deployment_input.components.loaders.workflow_loader import WorkflowLoader
from caribou.deployment_solver.models.indexer import Indexer
from caribou.deployment_solver.deployment_input.components.calculators.runtime_calculator import RuntimeCalculator


class TestRuntimeCalculator(unittest.TestCase):
    def setUp(self):
        # Mock the loaders
        self.performance_loader = MagicMock(spec=PerformanceLoader)
        self.workflow_loader = MagicMock(spec=WorkflowLoader)

        # Create an instance of RuntimeCalculator with mocks
        self.runtime_calculator = RuntimeCalculator(self.performance_loader, self.workflow_loader)

        # Define test data
        self.from_instance_name = "instance1"
        self.to_instance_name = "instance2"
        self.from_region_name = "region1"
        self.to_region_name = "region2"
        self.data_transfer_size = 1.0

    def test_reset_cache(self):
        # Ensure cache is cleared
        self.runtime_calculator.reset_cache()
        self.assertEqual(self.runtime_calculator._transmission_latency_distribution_cache, {})
        self.assertEqual(self.runtime_calculator._transmission_size_distribution_cache, {})

    @patch("random.random", return_value=0.0)
    def test_calculate_transmission_size_and_latency(self, mock_random):
        # Mock the distribution methods
        self.workflow_loader.get_data_transfer_size_distribution.return_value = [0.1, 0.2, 0.3]
        self.workflow_loader.get_latency_distribution.return_value = [0.4, 0.5, 0.6]

        # Call the method
        transmission_size, transmission_latency = self.runtime_calculator.calculate_transmission_size_and_latency(
            self.from_instance_name,
            self.from_region_name,
            self.to_instance_name,
            self.to_region_name,
            is_sync_predecessor=False,
            consider_from_client_latency=False,
        )

        # Verify results
        self.assertEqual(transmission_size, 0.1)
        self.assertEqual(transmission_latency, 0.4)

    @patch("random.random", return_value=0.0)
    def test_calculate_simulated_transmission_size_and_latency(self, mock_random):
        # Mock the distribution methods
        self.workflow_loader.get_non_execution_sns_transfer_size.return_value = 0.1
        self.workflow_loader.get_non_execution_transfer_latency_distribution.return_value = [0.2, 0.3, 0.4]

        # Call the method
        (
            transmission_size,
            transmission_latency,
        ) = self.runtime_calculator.calculate_simulated_transmission_size_and_latency(
            self.from_instance_name,
            self.to_instance_name,
            self.from_instance_name,
            self.to_instance_name,
            self.from_region_name,
            self.to_region_name,
        )

        # Verify results
        self.assertEqual(transmission_size, 0.1)
        self.assertEqual(transmission_latency, 0.2)

    @patch("random.random", return_value=0.0)
    def test_calculate_node_runtimes_and_data_transfer(self, mock_random):
        # Setup mock data
        self.workflow_loader.get_runtime_distribution.return_value = [5.0, 5.1, 5.2]
        self.workflow_loader.get_auxiliary_index_translation.return_value = {
            "data_transfer_during_execution_gb": 0,
            "successor_instance": 1,
        }
        self.workflow_loader.get_auxiliary_data_distribution.return_value = [[0.1, 0.2], [0.2, 0.3]]

        self.performance_loader.get_relative_performance.side_effect = (
            lambda x: 1.0 if x == self.from_region_name else 0.9
        )

        instance_indexer = MagicMock(spec=Indexer)
        instance_indexer.value_to_index.side_effect = lambda x: 1 if x == "successor_instance" else 0

        # Call the method
        (
            runtime_data,
            current_execution_time,
            data_transfer,
        ) = self.runtime_calculator.calculate_node_runtimes_and_data_transfer(
            self.from_instance_name,
            self.from_region_name,
            previous_cumulative_runtime=0.0,
            instance_indexer=instance_indexer,
            is_redirector=False,
        )

        # Verify results
        self.assertEqual(runtime_data["current"], 5.0)
        self.assertEqual(current_execution_time, 5.0)
        self.assertEqual(data_transfer, 0.1)

    def test_get_transmission_size_distribution(self):
        # Setup cache
        self.runtime_calculator._transmission_size_distribution_cache = {
            f"{self.from_instance_name}-{self.to_instance_name}": [0.1, 0.2, 0.3]
        }

        # Call the method
        size_distribution = self.runtime_calculator._get_transmission_size_distribution(
            self.from_instance_name, self.to_instance_name
        )

        # Verify cache was used
        self.assertEqual(size_distribution, [0.1, 0.2, 0.3])
        self.workflow_loader.get_data_transfer_size_distribution.assert_not_called()

    def test_get_transmission_latency_distribution(self):
        # Setup cache
        self.runtime_calculator._transmission_latency_distribution_cache = {
            f"{self.from_instance_name}-{self.to_instance_name}-{self.from_region_name}-{self.to_region_name}-{self.data_transfer_size}": [
                0.1,
                0.2,
                0.3,
            ]
        }

        # Call the method
        latency_distribution = self.runtime_calculator._get_transmission_latency_distribution(
            self.from_instance_name,
            self.from_region_name,
            self.to_instance_name,
            self.to_region_name,
            self.data_transfer_size,
            is_sync_predecessor=False,
            consider_from_client_latency=False,
        )

        # Verify cache was used
        self.assertEqual(latency_distribution, [0.1, 0.2, 0.3])
        self.workflow_loader.get_latency_distribution.assert_not_called()

    @patch("random.random", return_value=0.0)
    def test_handle_missing_transmission_latency_distribution(self, mock_random):
        # Mock the loader methods
        self.performance_loader.get_transmission_latency_distribution.return_value = [0.2, 0.3, 0.4]
        self.workflow_loader.get_home_region.return_value = self.from_region_name
        self.workflow_loader.get_latency_distribution.return_value = [0.5, 0.6, 0.7]

        # Call the method
        missing_distribution = self.runtime_calculator._handle_missing_transmission_latency_distribution(
            self.from_instance_name,
            self.from_region_name,
            self.to_instance_name,
            self.to_region_name,
            self.data_transfer_size,
            is_sync_predecessor=False,
        )

        # Verify results
        self.assertEqual(missing_distribution, [0.5, 0.6, 0.7])

    def test_handle_missing_start_hop_latency_distribution(self):
        # Mock the loader methods
        self.workflow_loader.get_home_region.return_value = self.from_region_name
        self.workflow_loader.get_start_hop_latency_distribution.return_value = [0.3, 0.4, 0.5]
        self.performance_loader.get_transmission_latency_distribution.return_value = [0.2, 0.3, 0.4]

        # Call the method
        start_hop_distribution = self.runtime_calculator._handle_missing_start_hop_latency_distribution(
            self.to_region_name, self.data_transfer_size
        )

        # Verify results
        self.assertEqual(start_hop_distribution, [0.5, 0.7, 0.9])

    def test_handle_missing_start_hop_latency_distribution_home_region(self):
        # Mock the loader methods
        self.workflow_loader.get_home_region.return_value = self.to_region_name

        # Expect a ValueError if the home region has no latency data
        with self.assertRaises(ValueError):
            self.runtime_calculator._handle_missing_start_hop_latency_distribution(
                self.to_region_name, self.data_transfer_size
            )

    def test_calculate_transmission_size_and_latency_empty_distributions(self):
        # Mock the distribution methods to return empty lists
        self.workflow_loader.get_data_transfer_size_distribution.return_value = []
        self.workflow_loader.get_latency_distribution.return_value = []

        with self.assertRaises(ValueError):
            self.runtime_calculator.calculate_transmission_size_and_latency(
                self.from_instance_name,
                self.from_region_name,
                self.to_instance_name,
                self.to_region_name,
                is_sync_predecessor=False,
                consider_from_client_latency=False,
            )

    def test_calculate_simulated_transmission_size_and_latency_empty_latency_distribution(self):
        # Mock the distribution methods
        self.workflow_loader.get_non_execution_sns_transfer_size.return_value = 0.1
        self.workflow_loader.get_non_execution_transfer_latency_distribution.return_value = []

        # Mock fallback method
        self.runtime_calculator._get_transmission_latency_distribution = MagicMock(return_value=[0.25])

        # Call the method
        (
            transmission_size,
            transmission_latency,
        ) = self.runtime_calculator.calculate_simulated_transmission_size_and_latency(
            self.from_instance_name,
            self.to_instance_name,
            self.from_instance_name,
            self.to_instance_name,
            self.from_region_name,
            self.to_region_name,
        )

        # Verify fallback was used
        self.assertEqual(transmission_size, 0.1)
        self.assertEqual(transmission_latency, 0.25)

    @patch("random.random", return_value=0.0)
    def test_calculate_node_runtimes_and_data_transfer_empty_runtime_distribution(self, mock_random):
        # Setup mocks
        self.workflow_loader.get_home_region.return_value = "home_region"
        self.workflow_loader.get_runtime_distribution.side_effect = [
            [],  # First call: No data in original region
            [5.0],  # Second call: Data in home region
        ]
        self.workflow_loader.get_auxiliary_index_translation.return_value = {
            "data_transfer_during_execution_gb": 0,
            "successor_instance": 1,
        }
        self.workflow_loader.get_auxiliary_data_distribution.return_value = [[0.0, 0.2], [0.1, 0.3]]

        # Mocking get_relative_performance to return a concrete value
        self.performance_loader.get_relative_performance.return_value = 1.0

        instance_indexer = MagicMock(spec=Indexer)
        instance_indexer.value_to_index.side_effect = lambda x: 1 if x == "successor_instance" else 0

        # Call the method
        (
            runtime_data,
            current_execution_time,
            data_transfer,
        ) = self.runtime_calculator.calculate_node_runtimes_and_data_transfer(
            "instance1",
            "region1",
            previous_cumulative_runtime=0.0,
            instance_indexer=instance_indexer,
            is_redirector=False,
        )

        # Verify that the fallback to home region worked
        self.assertEqual(runtime_data["current"], 5.0)
        self.assertEqual(current_execution_time, 5.0)
        self.assertEqual(data_transfer, 0.0)

        # Ensure `get_runtime_distribution` was called for both the original and home regions
        self.workflow_loader.get_runtime_distribution.assert_any_call("instance1", "home_region", False)


if __name__ == "__main__":
    unittest.main()
