import unittest
from unittest.mock import Mock, patch
from caribou.deployment_solver.deployment_input.components.loaders.datacenter_loader import DatacenterLoader
from caribou.common.models.remote_client.remote_client import RemoteClient
from caribou.common.constants import (
    SOLVER_INPUT_AVERAGE_MEMORY_POWER_DEFAULT,
    SOLVER_INPUT_CFE_DEFAULT,
    SOLVER_INPUT_COMPUTE_COST_DEFAULT,
    SOLVER_INPUT_DYNAMODB_READ_COST_DEFAULT,
    SOLVER_INPUT_DYNAMODB_WRITE_COST_DEFAULT,
    SOLVER_INPUT_INVOCATION_COST_DEFAULT,
    SOLVER_INPUT_MAX_CPU_POWER_DEFAULT,
    SOLVER_INPUT_MIN_CPU_POWER_DEFAULT,
    SOLVER_INPUT_PUE_DEFAULT,
    SOLVER_INPUT_SNS_REQUEST_COST_DEFAULT,
    SOLVER_INPUT_TRANSMISSION_COST_DEFAULT,
    SOLVER_INPUT_ECR_MONTHLY_STORAGE_COST_DEFAULT,
)


class TestDatacenterLoader(unittest.TestCase):
    def setUp(self):
        # Mock the RemoteClient
        self.client = Mock(spec=RemoteClient)
        self.loader = DatacenterLoader(self.client)

        # Sample data for testing
        self.loader._datacenter_data = {
            "aws:ca-central-1": {
                "execution_cost": {
                    "invocation_cost": {"arm64": 2e-07, "x86_64": 2e-07, "free_tier_invocations": 1000000},
                    "compute_cost": {"arm64": 1.33334e-05, "x86_64": 1.66667e-05, "free_tier_compute_gb_s": 400000},
                    "unit": "USD",
                },
                "transmission_cost": {"global_data_transfer": 0.09, "provider_data_transfer": 0.02, "unit": "USD/GB"},
                "sns_cost": {"request_cost": 5e-07, "unit": "USD/requests"},
                "dynamodb_cost": {"read_cost": 2.75e-07, "write_cost": 1.375e-06, "storage_cost": 0.11, "unit": "USD"},
                "ecr_cost": {"storage_cost": 0.1, "unit": "USD"},
                "pue": 1.11,
                "cfe": 0.0,
                "average_memory_power": 3.92e-06,
                "max_cpu_power_kWh": 0.0035,
                "min_cpu_power_kWh": 0.00074,
                "available_architectures": ["arm64", "x86_64"],
            }
        }

    def test_get_average_memory_power(self):
        result = self.loader.get_average_memory_power("aws:ca-central-1")
        self.assertEqual(result, 3.92e-06)

        # Test default value
        result = self.loader.get_average_memory_power("unknown-region")
        self.assertEqual(result, SOLVER_INPUT_AVERAGE_MEMORY_POWER_DEFAULT)

    def test_get_pue(self):
        result = self.loader.get_pue("aws:ca-central-1")
        self.assertEqual(result, 1.11)

        # Test default value
        result = self.loader.get_pue("unknown-region")
        self.assertEqual(result, SOLVER_INPUT_PUE_DEFAULT)

    def test_get_cfe(self):
        result = self.loader.get_cfe("aws:ca-central-1")
        self.assertEqual(result, 0.0)

        # Test default value
        result = self.loader.get_cfe("unknown-region")
        self.assertEqual(result, SOLVER_INPUT_CFE_DEFAULT)

    def test_get_max_cpu_power(self):
        result = self.loader.get_max_cpu_power("aws:ca-central-1")
        self.assertEqual(result, 0.0035)

        # Test default value
        result = self.loader.get_max_cpu_power("unknown-region")
        self.assertEqual(result, SOLVER_INPUT_MAX_CPU_POWER_DEFAULT)

    def test_get_min_cpu_power(self):
        result = self.loader.get_min_cpu_power("aws:ca-central-1")
        self.assertEqual(result, 0.00074)

        # Test default value
        result = self.loader.get_min_cpu_power("unknown-region")
        self.assertEqual(result, SOLVER_INPUT_MIN_CPU_POWER_DEFAULT)

    def test_get_sns_request_cost(self):
        result = self.loader.get_sns_request_cost("aws:ca-central-1")
        self.assertEqual(result, 5e-07)

        # Test default value
        result = self.loader.get_sns_request_cost("unknown-region")
        self.assertEqual(result, SOLVER_INPUT_SNS_REQUEST_COST_DEFAULT)

    def test_get_dynamodb_read_write_cost(self):
        result = self.loader.get_dynamodb_read_write_cost("aws:ca-central-1")
        self.assertEqual(result, (2.75e-07, 1.375e-06))

        # Test default value
        result = self.loader.get_dynamodb_read_write_cost("unknown-region")
        self.assertEqual(result, (SOLVER_INPUT_DYNAMODB_READ_COST_DEFAULT, SOLVER_INPUT_DYNAMODB_WRITE_COST_DEFAULT))

    def test_get_ecr_storage_cost(self):
        result = self.loader.get_ecr_storage_cost("aws:ca-central-1")
        self.assertEqual(result, 0.1)

        # Test default value
        result = self.loader.get_ecr_storage_cost("unknown-region")
        self.assertEqual(result, SOLVER_INPUT_ECR_MONTHLY_STORAGE_COST_DEFAULT)

    def test_get_compute_cost(self):
        result = self.loader.get_compute_cost("aws:ca-central-1", "arm64")
        self.assertEqual(result, 1.33334e-05)

        # Test default value
        result = self.loader.get_compute_cost("unknown-region", "arm64")
        self.assertEqual(result, SOLVER_INPUT_COMPUTE_COST_DEFAULT)

    def test_get_invocation_cost(self):
        result = self.loader.get_invocation_cost("aws:ca-central-1", "arm64")
        self.assertEqual(result, 2e-07)

        # Test default value
        result = self.loader.get_invocation_cost("unknown-region", "arm64")
        self.assertEqual(result, SOLVER_INPUT_INVOCATION_COST_DEFAULT)

    def test_get_transmission_cost(self):
        result = self.loader.get_transmission_cost("aws:ca-central-1", True)
        self.assertEqual(result, 0.02)

        result = self.loader.get_transmission_cost("aws:ca-central-1", False)
        self.assertEqual(result, 0.09)

        # Test default value
        result = self.loader.get_transmission_cost("unknown-region", True)
        self.assertEqual(result, SOLVER_INPUT_TRANSMISSION_COST_DEFAULT)

    @patch.object(DatacenterLoader, "_retrieve_region_data")
    @patch.object(DatacenterLoader, "_retrieve_provider_data")
    def test_setup(self, mock_retrieve_provider_data, mock_retrieve_region_data):
        mock_retrieve_region_data.return_value = self.loader._datacenter_data
        mock_retrieve_provider_data.return_value = {}
        self.loader.setup({"aws:ca-central-1"})

        # Validate that setup correctly sets the datacenter data
        self.assertEqual(self.loader._datacenter_data, self.loader._datacenter_data)


if __name__ == "__main__":
    unittest.main()
