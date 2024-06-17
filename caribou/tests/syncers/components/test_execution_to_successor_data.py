import unittest
from typing import Any, Optional
from caribou.syncers.components.execution_to_successor_data import ExecutionToSuccessorData


class TestExecutionToSuccessorData(unittest.TestCase):
    def setUp(self):
        self.successor_instance_name = "test_successor"
        self.execution_data = ExecutionToSuccessorData(self.successor_instance_name)

    def test_init(self):
        self.assertEqual(self.execution_data.successor_instance_name, self.successor_instance_name)
        self.assertIsNone(self.execution_data.task_type)
        self.assertIsNone(self.execution_data.invocation_time_from_function_start)
        self.assertIsNone(self.execution_data.finish_time_from_function_start)
        self.assertIsNone(self.execution_data.payload_data_size)
        self.assertIsNone(self.execution_data.upload_data_size)
        self.assertIsNone(self.execution_data.consumed_write_capacity)
        self.assertIsNone(self.execution_data.sync_data_response_size)
        self.assertIsNone(self.execution_data.destination_region)
        self.assertEqual(self.execution_data.invoking_sync_node_data_output, {})

    def test_get_total_output_data_size(self):
        self.execution_data.payload_data_size = 1.0
        self.execution_data.upload_data_size = 2.0
        self.execution_data.invoking_sync_node_data_output = {
            "node1": {"data_transfer_size": 3.0},
            "node2": {"data_transfer_size": 4.0},
        }
        self.assertEqual(self.execution_data.get_total_output_data_size(), 10.0)

    def test_get_total_output_data_size_none(self):
        self.execution_data.payload_data_size = None
        self.execution_data.upload_data_size = None
        self.execution_data.invoking_sync_node_data_output = {}
        self.assertEqual(self.execution_data.get_total_output_data_size(), 0.0)

    def test_get_total_input_data_size(self):
        self.execution_data.sync_data_response_size = 5.0
        self.assertEqual(self.execution_data.get_total_input_data_size(), 5.0)

    def test_get_total_input_data_size_none(self):
        self.execution_data.sync_data_response_size = None
        self.assertEqual(self.execution_data.get_total_input_data_size(), 0.0)

    def test_to_dict(self):
        self.execution_data.task_type = "test_task"
        self.execution_data.invocation_time_from_function_start = 1.5
        self.execution_data.consumed_write_capacity = 2.5
        self.execution_data.sync_data_response_size = 3.5
        self.execution_data.destination_region = "us-west-2"

        expected_dict = {
            "task_type": "test_task",
            "invocation_time_from_function_start_s": 1.5,
        }

        self.assertEqual(self.execution_data.to_dict(), expected_dict)

    def test_to_dict_none_fields(self):
        self.execution_data.task_type = None
        self.execution_data.invocation_time_from_function_start = None
        self.execution_data.consumed_write_capacity = None
        self.execution_data.sync_data_response_size = None
        self.execution_data.destination_region = None

        expected_dict = {}

        self.assertEqual(self.execution_data.to_dict(), expected_dict)

    def test_get_total_output_data_size_with_invoking_sync_node_data_output(self):
        self.execution_data.payload_data_size = 1.0
        self.execution_data.upload_data_size = 2.0
        self.execution_data.invoking_sync_node_data_output = {
            "node1": {"data_transfer_size": 3.0},
            "node2": {"data_transfer_size": 4.0},
        }
        self.assertEqual(self.execution_data.get_total_output_data_size(), 10.0)

    def test_get_total_output_data_size_with_invoking_sync_node_data_output_none(self):
        self.execution_data.payload_data_size = 1.0
        self.execution_data.upload_data_size = 2.0
        self.execution_data.invoking_sync_node_data_output = {
            "node1": {"data_transfer_size": 0.0},
            "node2": {"data_transfer_size": 0.0},
        }
        self.assertEqual(self.execution_data.get_total_output_data_size(), 3.0)

    def test_to_dict_with_invoking_sync_node_data_output(self):
        self.execution_data.task_type = "test_task"
        self.execution_data.invocation_time_from_function_start = 1.5
        self.execution_data.invoking_sync_node_data_output = {
            "node1": {"data_transfer_size": 3.0},
            "node2": {"data_transfer_size": 4.0},
        }

        expected_dict = {
            "task_type": "test_task",
            "invocation_time_from_function_start_s": 1.5,
            "sync_info": {"node1": {"data_transfer_size": 3.0}, "node2": {"data_transfer_size": 4.0}},
        }

        self.assertEqual(self.execution_data.to_dict(), expected_dict)


if __name__ == "__main__":
    unittest.main()
