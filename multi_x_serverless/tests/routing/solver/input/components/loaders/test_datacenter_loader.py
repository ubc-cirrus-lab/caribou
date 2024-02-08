import unittest
from unittest.mock import Mock, patch
from multi_x_serverless.routing.solver.input.components.loaders.datacenter_loader import DatacenterLoader

class TestDatacenterLoader(unittest.TestCase):
    def setUp(self):
        self.client = Mock()
        self.loader = DatacenterLoader(self.client)
        self.loader._datacenter_data = {
            "aws:region1": {
                "execution_cost": {
                    "invocation_cost": {"arm64": 2.3e-7, "x86_64": 2.3e-7, "free_tier_invocations": 1000000},
                    "compute_cost": {"arm64": 1.56138e-5, "x86_64": 1.95172e-5, "free_tier_compute_gb_s": 400000},
                    "unit": "USD",
                },
                "transmission_cost": {"global_data_transfer": 0.09, "provider_data_transfer": 0.02, "unit": "USD/GB"},
                "pue": 1.15,
                "cfe": 0.9,
                "average_memory_power": 3.92e-6,
                "average_cpu_power": 0.00212,
                "available_architectures": ["arm64", "x86_64"],
            }
        }

    def test_init(self):
        self.assertEqual(self.loader._client, self.client)

    @patch('multi_x_serverless.routing.solver.input.components.loaders.datacenter_loader.DatacenterLoader._retrieve_region_data')
    @patch('multi_x_serverless.routing.solver.input.components.loaders.datacenter_loader.DatacenterLoader._retrieve_provider_data')
    def test_setup(self, mock_retrieve_provider_data, mock_retrieve_region_data):
        mock_retrieve_region_data.return_value = self.loader._datacenter_data
        mock_retrieve_provider_data.return_value = {}
        self.loader.setup({"aws:region1"})
        self.assertEqual(self.loader._datacenter_data, self.loader._datacenter_data)

    def test_get_average_cpu_power(self):
        result = self.loader.get_average_cpu_power("aws:region1")
        self.assertEqual(result, 0.00212)

    def test_get_average_memory_power(self):
        result = self.loader.get_average_memory_power("aws:region1")
        self.assertEqual(result, 3.92e-6)

    def test_get_pue(self):
        result = self.loader.get_pue("aws:region1")
        self.assertEqual(result, 1.15)

    def test_get_cfe(self):
        result = self.loader.get_cfe("aws:region1")
        self.assertEqual(result, 0.9)

    def test_get_compute_cost(self):
        result = self.loader.get_compute_cost("aws:region1", "arm64")
        self.assertEqual(result, 1.56138e-5)

    def test_get_invocation_cost(self):
        result = self.loader.get_invocation_cost("aws:region1", "arm64")
        self.assertEqual(result, 2.3e-7)

    def test_get_transmission_cost(self):
        result = self.loader.get_transmission_cost("aws:region1", True)
        self.assertEqual(result, 0.02)

if __name__ == '__main__':
    unittest.main()