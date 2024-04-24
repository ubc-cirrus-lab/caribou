import unittest
from unittest.mock import Mock, patch
from caribou.deployment_solver.deployment_input.components.loaders.performance_loader import PerformanceLoader


class TestPerformanceLoader(unittest.TestCase):
    def setUp(self):
        self.client = Mock()
        self.loader = PerformanceLoader(self.client)
        self.loader._performance_data = {
            "aws:region1": {
                "relative_performance": 1,
                "transmission_latency": {
                    "aws:region1": {"latency_distribution": [0.005], "unit": "s"},
                    "aws:region2": {"latency_distribution": [0.05], "unit": "s"},
                },
            }
        }

    def test_init(self):
        self.assertEqual(self.loader._client, self.client)

    @patch.object(PerformanceLoader, "_retrieve_region_data")
    def test_setup(self, mock_retrieve_region_data):
        mock_retrieve_region_data.return_value = self.loader._performance_data
        self.loader.setup({"aws:region1"})
        self.assertEqual(self.loader._performance_data, self.loader._performance_data)

    def test_get_relative_performance(self):
        result = self.loader.get_relative_performance("aws:region1")
        self.assertEqual(result, 1)

    def test_get_transmission_latency_distribution(self):
        result = self.loader.get_transmission_latency_distribution("aws:region1", "aws:region2")
        self.assertEqual(result, [0.05])

    def test_get_performance_data(self):
        result = self.loader.get_performance_data()
        self.assertEqual(result, self.loader._performance_data)


if __name__ == "__main__":
    unittest.main()
