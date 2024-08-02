import unittest
from unittest.mock import MagicMock, patch
import math
from caribou.deployment_solver.deployment_input.components.calculators.cost_calculator import CostCalculator
from caribou.deployment_solver.deployment_input.components.loaders.datacenter_loader import DatacenterLoader
from caribou.deployment_solver.deployment_input.components.loaders.workflow_loader import WorkflowLoader


class TestCostCalculator(unittest.TestCase):
    def setUp(self):
        # Create mocks for the datacenter and workflow loaders
        self.datacenter_loader = MagicMock(spec=DatacenterLoader)
        self.workflow_loader = MagicMock(spec=WorkflowLoader)

        # Create an instance of CostCalculator with mocks
        self.cost_calculator = CostCalculator(
            datacenter_loader=self.datacenter_loader,
            workflow_loader=self.workflow_loader,
            consider_intra_region_transfer_for_sns=True,
        )

    def test_calculate_virtual_start_instance_cost(self):
        # Mock the methods for DynamoDB and SNS costs
        self.datacenter_loader.get_dynamodb_read_write_cost.return_value = (0.02, 0.03)
        self.datacenter_loader.get_sns_request_cost.return_value = 0.001

        # Define the test data
        sns_data_call_and_output_sizes = {"aws:us-east-1": [0.005, 0.01]}
        dynamodb_read_capacity = 100
        dynamodb_write_capacity = 200

        # Call the method under test
        cost = self.cost_calculator.calculate_virtual_start_instance_cost(
            data_output_sizes={},
            sns_data_call_and_output_sizes=sns_data_call_and_output_sizes,
            dynamodb_read_capacity=dynamodb_read_capacity,
            dynamodb_write_capacity=dynamodb_write_capacity,
        )

        # Calculate expected cost
        expected_dynamodb_cost = 100 * 0.02 + 200 * 0.03
        expected_sns_cost = sum(
            math.ceil(size * 1024**2 / 64) * 0.001 for size in sns_data_call_and_output_sizes["aws:us-east-1"]
        )
        expected_cost = expected_dynamodb_cost + expected_sns_cost

        # Assert the cost calculation is correct
        self.assertEqual(cost, expected_cost)

    def test_calculate_instance_cost_invoked(self):
        # Mock the methods for DynamoDB, SNS, execution costs, and data transfer
        self.cost_calculator._calculate_execution_cost = MagicMock(return_value=0.5)
        self.cost_calculator._calculate_sns_cost = MagicMock(return_value=0.3)
        self.cost_calculator._calculate_data_transfer_cost = MagicMock(return_value=0.4)
        self.cost_calculator._calculate_dynamodb_cost = MagicMock(return_value=0.2)

        # Define the test data
        execution_time = 100.0
        instance_name = "test_instance"
        current_region_name = "aws:us-west-2"
        data_output_sizes = {"aws:us-east-1": 0.1}
        sns_data_call_and_output_sizes = {"aws:us-east-1": [0.02, 0.04]}
        dynamodb_read_capacity = 50.0
        dynamodb_write_capacity = 100.0
        is_invoked = True

        # Call the method under test
        cost = self.cost_calculator.calculate_instance_cost(
            execution_time,
            instance_name,
            current_region_name,
            data_output_sizes,
            sns_data_call_and_output_sizes,
            dynamodb_read_capacity,
            dynamodb_write_capacity,
            is_invoked,
        )

        # Calculate expected cost
        expected_cost = 0.5 + 0.3 + 0.4 + 0.2

        # Assert the cost calculation is correct
        self.assertEqual(cost, expected_cost)

    def test_calculate_instance_cost_not_invoked(self):
        # Mock the methods for data transfer and DynamoDB costs
        self.cost_calculator._calculate_data_transfer_cost = MagicMock(return_value=0.4)
        self.cost_calculator._calculate_dynamodb_cost = MagicMock(return_value=0.2)

        # Define the test data
        execution_time = 0.0
        instance_name = "test_instance"
        current_region_name = "aws:us-west-2"
        data_output_sizes = {"aws:us-east-1": 0.1}
        sns_data_call_and_output_sizes = {"aws:us-east-1": [0.02, 0.04]}
        dynamodb_read_capacity = 50.0
        dynamodb_write_capacity = 100.0
        is_invoked = False

        # Call the method under test
        cost = self.cost_calculator.calculate_instance_cost(
            execution_time,
            instance_name,
            current_region_name,
            data_output_sizes,
            sns_data_call_and_output_sizes,
            dynamodb_read_capacity,
            dynamodb_write_capacity,
            is_invoked,
        )

        # Calculate expected cost
        expected_cost = 0.4 + 0.2

        # Assert the cost calculation is correct
        self.assertEqual(cost, expected_cost)

    def test_calculate_dynamodb_cost(self):
        # Mock the methods for DynamoDB read/write costs
        self.datacenter_loader.get_dynamodb_read_write_cost.return_value = (0.02, 0.03)

        # Define the test data
        current_region_name = "aws:us-east-1"
        dynamodb_read_capacity = 50
        dynamodb_write_capacity = 100

        # Call the private method under test
        cost = self.cost_calculator._calculate_dynamodb_cost(
            current_region_name, dynamodb_read_capacity, dynamodb_write_capacity
        )

        # Calculate expected cost
        expected_cost = 50 * 0.02 + 100 * 0.03

        # Assert the cost calculation is correct
        self.assertEqual(cost, expected_cost)

    def test_calculate_sns_cost(self):
        # Mock the SNS request cost and transmission cost methods
        self.datacenter_loader.get_sns_request_cost.return_value = 0.001
        self.datacenter_loader.get_transmission_cost.return_value = 0.05

        # Define the test data
        current_region_name = "aws:us-west-2"
        sns_data_call_and_output_sizes = {"aws:us-west-2": [0.005, 0.01], "aws:us-east-1": [0.002, 0.004]}

        # Call the private method under test
        cost = self.cost_calculator._calculate_sns_cost(current_region_name, sns_data_call_and_output_sizes)

        # Calculate expected cost
        expected_cost = 0
        # Account for intra-region SNS data transfer
        if self.cost_calculator._consider_intra_region_transfer_for_sns:
            expected_cost += sum(sns_data_call_and_output_sizes["aws:us-west-2"]) * 0.05
        # Calculate SNS invocation costs
        for region, sizes in sns_data_call_and_output_sizes.items():
            for size in sizes:
                requests = math.ceil(size * 1024**2 / 64)
                expected_cost += requests * 0.001

        # Assert the cost calculation is correct using assertAlmostEqual for precision
        self.assertAlmostEqual(cost, expected_cost, places=3)

    def test_calculate_data_transfer_cost(self):
        # Mock the transmission cost method
        self.datacenter_loader.get_transmission_cost.return_value = 0.05

        # Define the test data
        current_region_name = "aws:us-west-2"
        data_output_sizes = {"aws:us-east-1": 0.1, "aws:us-west-2": 0.2}

        # Call the private method under test
        cost = self.cost_calculator._calculate_data_transfer_cost(current_region_name, data_output_sizes)

        # Calculate expected cost
        expected_cost = 0.1 * 0.05  # Only count data transfer out of the current region

        # Assert the cost calculation is correct
        self.assertEqual(cost, expected_cost)

    def test_calculate_execution_cost(self):
        # Mock the method for execution conversion ratio
        self.cost_calculator._get_execution_conversion_ratio = MagicMock(return_value=(0.1, 0.05))

        # Define the test data
        instance_name = "test_instance"
        region_name = "aws:us-west-2"
        execution_time = 10.0

        # Call the private method under test
        cost = self.cost_calculator._calculate_execution_cost(instance_name, region_name, execution_time)

        # Calculate expected cost
        expected_cost = 0.1 * 10.0 + 0.05

        # Assert the cost calculation is correct
        self.assertEqual(cost, expected_cost)

    def test_get_execution_conversion_ratio(self):
        # Mock the method for getting memory and architecture
        self.workflow_loader.get_memory.return_value = 2048
        self.workflow_loader.get_architecture.return_value = "x86_64"
        self.datacenter_loader.get_compute_cost.return_value = 0.2
        self.datacenter_loader.get_invocation_cost.return_value = 0.05

        # Define the test data
        instance_name = "test_instance"
        region_name = "aws:us-west-2"

        # Call the private method under test
        ratio = self.cost_calculator._get_execution_conversion_ratio(instance_name, region_name)

        # Calculate expected ratio
        expected_ratio = (0.2 * (2048 / 1024), 0.05)

        # Assert the conversion ratio calculation is correct
        self.assertEqual(ratio, expected_ratio)

        # Assert the cache is updated correctly
        cache_key = f"{instance_name}_{region_name}"
        self.assertIn(cache_key, self.cost_calculator._execution_conversion_ratio_cache)
        self.assertEqual(self.cost_calculator._execution_conversion_ratio_cache[cache_key], expected_ratio)


if __name__ == "__main__":
    unittest.main()
