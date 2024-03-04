import unittest
from unittest.mock import Mock

from multi_x_serverless.routing.deployment_input.components.calculators.runtime_calculator import RuntimeCalculator


class TestRuntimeCalculator(unittest.TestCase):
    def setUp(self):
        self.performance_loader = Mock()
        self.workflow_loader = Mock()
        self.runtime_calculator = RuntimeCalculator(self.performance_loader, self.workflow_loader)

    def test_calculate_runtime(self):
        self.runtime_calculator.calculate_raw_runtime = Mock(return_value=10.0)
        result = self.runtime_calculator.calculate_runtime("instance1", "region1")
        self.assertEqual(result, 10.0)

    def test_calculate_latency(self):
        self.runtime_calculator.calculate_raw_latency = Mock(return_value=20.0)
        result = self.runtime_calculator.calculate_latency("instance1", "instance2", "region1", "region2")
        self.assertEqual(result, 20.0)

    def test_calculate_raw_runtime(self):
        self.workflow_loader.get_runtime.return_value = 1.0
        self.workflow_loader.get_favourite_region.return_value = "region1"
        self.workflow_loader.get_favourite_region_runtime.return_value = 1.0
        self.performance_loader.get_relative_performance.return_value = 1.0

        result = self.runtime_calculator.calculate_raw_runtime("instance1", "region1")
        self.assertEqual(result, 1.0)

    def test__calculate_raw_latency(self):
        self.workflow_loader.get_latency.return_value = 1.0
        self.workflow_loader.get_data_transfer_size.return_value = 1.0
        self.performance_loader.get_transmission_latency.return_value = 1.0

        result = self.runtime_calculator.calculate_raw_latency("instance1", "instance2", "region1", "region2")
        self.assertEqual(result, 1.0)


if __name__ == "__main__":
    unittest.main()
