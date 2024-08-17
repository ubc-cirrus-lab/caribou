import unittest
from unittest.mock import Mock, patch, PropertyMock
from typing import Any, Optional
from caribou.syncers.components.execution_data import ExecutionData
from caribou.syncers.components.execution_to_successor_data import ExecutionToSuccessorData


class TestExecutionData(unittest.TestCase):
    def setUp(self):
        self.instance_name = "test_instance"
        self.execution_data = ExecutionData(self.instance_name)

    def test_init(self):
        self.assertEqual(self.execution_data.instance_name, self.instance_name)
        self.assertEqual(self.execution_data.input_payload_size, 0.0)
        self.assertIsNone(self.execution_data.request_id)
        self.assertIsNone(self.execution_data.cpu_model)
        self.assertIsNone(self.execution_data.user_execution_duration)
        self.assertIsNone(self.execution_data.execution_duration)
        self.assertIsNone(self.execution_data.provider_region)
        self.assertIsNone(self.execution_data.download_size)
        self.assertIsNone(self.execution_data.download_time)
        self.assertIsNone(self.execution_data.consumed_read_capacity)
        self.assertIsNone(self.execution_data.lambda_insights)
        self.assertEqual(self.execution_data.successor_data, {})

    def test_get_successor_data(self):
        successor_instance_name = "successor_instance"
        successor_data = self.execution_data.get_successor_data(successor_instance_name)
        self.assertIsInstance(successor_data, ExecutionToSuccessorData)
        self.assertEqual(successor_data.successor_instance_name, successor_instance_name)

    def test_get_total_output_data_size(self):
        successor_data = Mock(spec=ExecutionToSuccessorData)
        successor_data.get_total_output_data_size.return_value = 10.0
        self.execution_data.successor_data["successor1"] = successor_data
        self.assertEqual(self.execution_data._get_total_output_data_size(), 10.0)

    def test_get_total_input_data_size(self):
        self.execution_data.input_payload_size = 1.0
        self.execution_data.download_size = 2.0
        successor_data = Mock(spec=ExecutionToSuccessorData)
        successor_data.get_total_input_data_size.return_value = 3.0
        self.execution_data.successor_data["successor1"] = successor_data
        self.assertEqual(self.execution_data._get_total_input_data_size(), 6.0)

    def test_data_transfer_during_execution(self):
        self.execution_data.lambda_insights = {"total_network": 10 * 1024**3}  # 10 GB
        self.execution_data.input_payload_size = 1.0
        self.execution_data.download_size = 2.0
        successor_data = Mock(spec=ExecutionToSuccessorData)
        successor_data.get_total_input_data_size.return_value = 3.0
        successor_data.get_total_output_data_size.return_value = 4.0
        self.execution_data.successor_data["successor1"] = successor_data
        self.assertEqual(self.execution_data.data_transfer_during_execution, 10 - 6 - 4)

    def test_longest_duration(self):
        self.execution_data.user_execution_duration = 1.0
        self.execution_data.execution_duration = 2.0
        self.execution_data.lambda_insights = {"duration": 3.0}
        self.assertEqual(self.execution_data.longest_duration, 3.0)

    def test_longest_duration_none(self):
        self.assertEqual(self.execution_data.longest_duration, 0.0)

    def test_cpu_utilization(self):
        self.execution_data.lambda_insights = {"cpu_total_time": 100.0, "total_memory": 1769}  # 1 vCPU
        self.execution_data.execution_duration = 100.0
        self.assertEqual(self.execution_data.cpu_utilization, 1.0)

    def test_cpu_utilization_none(self):
        self.assertIsNone(self.execution_data.cpu_utilization)

    def test_is_completed(self):
        self.execution_data.request_id = "request_id"
        self.execution_data.lambda_insights = {"some_key": "some_value"}
        self.assertTrue(self.execution_data.is_completed)

    def test_is_not_completed(self):
        self.assertFalse(self.execution_data.is_completed)

    def test_to_dict(self):
        self.execution_data.cpu_model = "test_cpu"
        self.execution_data.provider_region = "us-west-2"
        self.execution_data.input_payload_size = 1.0
        self.execution_data.download_size = 2.0
        self.execution_data.lambda_insights = {
            "total_network": 10 * 1024**3,  # 10 GB
            "duration": 3.0,
            "cpu_total_time": 3.0,
            "total_memory": 1769,  # 1 vCPU
        }
        successor_data = Mock(spec=ExecutionToSuccessorData)
        successor_data.to_dict.return_value = {"key": "value"}
        successor_data.get_total_input_data_size.return_value = 3.0
        successor_data.get_total_output_data_size.return_value = 4.0
        self.execution_data.successor_data["successor1"] = successor_data

        expected_dict = {
            "instance_name": self.instance_name,
            "duration_s": 3.0,
            "cpu_model": "test_cpu",
            "provider_region": "us-west-2",
            "data_transfer_during_execution_gb": 10 - 6 - 4,
            "cpu_utilization": 1.0,
            "successor_data": {"successor1": {"key": "value"}},
            "additional_analysis_data": {
                "input_payload_size_gb": 1.0,
                "download_size_gb": 2.0,
                "total_input_data_transfer_gb": 6.0,
                "total_output_data_transfer_gb": 4.0,
            },
        }

        self.assertEqual(self.execution_data.to_dict(), expected_dict)


if __name__ == "__main__":
    unittest.main()
