import unittest
from caribou.syncers.components.execution_to_successor_data import ExecutionToSuccessorData


class TestExecutionToSuccessorData(unittest.TestCase):
    def setUp(self) -> None:
        # Initialize a common instance to use in tests
        self.execution_data = ExecutionToSuccessorData(successor_instance_name="instance_1")
        self.execution_data.task_type = "TaskTypeA"
        self.execution_data.invocation_time_from_function_start = 1.5
        self.execution_data.finish_time_from_invocation_start = 3.0
        self.execution_data.output_payload_data_size = 100.0
        self.execution_data.upload_data_size = 50.0
        self.execution_data.consumed_write_capacity = 25.0
        self.execution_data.sync_data_response_size = 75.0
        self.execution_data.destination_region = "us-west-2"
        self.execution_data.invoking_sync_node_data_output = {
            "node_1": {"data_transfer_size": 200.0},
            "node_2": {"data_transfer_size": 150.0},
        }

    def test_get_total_output_data_size(self) -> None:
        # Test calculation of total output data size
        total_output_size = self.execution_data.get_total_output_data_size()
        expected_output_size = (
            self.execution_data.output_payload_data_size
            + self.execution_data.upload_data_size
            + sum(
                data.get("data_transfer_size", 0.0)
                for data in self.execution_data.invoking_sync_node_data_output.values()
            )
        )
        self.assertEqual(total_output_size, expected_output_size)

    def test_get_total_output_data_size_empty(self) -> None:
        # Test calculation of total output data size when no data sizes are set
        empty_execution_data = ExecutionToSuccessorData(successor_instance_name="instance_empty")
        total_output_size = empty_execution_data.get_total_output_data_size()
        self.assertEqual(total_output_size, 0.0)

    def test_get_total_input_data_size(self) -> None:
        # Test calculation of total input data size
        total_input_size = self.execution_data.get_total_input_data_size()
        expected_input_size = self.execution_data.sync_data_response_size
        self.assertEqual(total_input_size, expected_input_size)

    def test_get_total_input_data_size_empty(self) -> None:
        # Test calculation of total input data size when no data sizes are set
        empty_execution_data = ExecutionToSuccessorData(successor_instance_name="instance_empty")
        total_input_size = empty_execution_data.get_total_input_data_size()
        self.assertEqual(total_input_size, 0.0)

    def test_to_dict(self) -> None:
        # Test conversion to dictionary with non-None values
        result_dict = self.execution_data.to_dict()
        expected_dict = {
            "task_type": self.execution_data.task_type,
            "invocation_time_from_function_start_s": self.execution_data.invocation_time_from_function_start,
            "sync_info": self.execution_data.invoking_sync_node_data_output,
            "consumed_write_capacity": self.execution_data.consumed_write_capacity,
        }
        self.assertEqual(result_dict, expected_dict)

    def test_to_dict_empty(self) -> None:
        # Test conversion to dictionary with all None values
        empty_execution_data = ExecutionToSuccessorData(successor_instance_name="instance_empty")
        result_dict = empty_execution_data.to_dict()
        expected_dict = {}
        self.assertEqual(result_dict, expected_dict)

    def test_initialization(self) -> None:
        # Test the proper initialization of an instance
        execution_data = ExecutionToSuccessorData(successor_instance_name="instance_2")
        self.assertEqual(execution_data.successor_instance_name, "instance_2")
        self.assertIsNone(execution_data.task_type)
        self.assertIsNone(execution_data.invocation_time_from_function_start)
        self.assertIsNone(execution_data.finish_time_from_invocation_start)
        self.assertIsNone(execution_data.output_payload_data_size)
        self.assertIsNone(execution_data.upload_data_size)
        self.assertIsNone(execution_data.consumed_write_capacity)
        self.assertIsNone(execution_data.sync_data_response_size)
        self.assertIsNone(execution_data.destination_region)
        self.assertEqual(execution_data.invoking_sync_node_data_output, {})

    def test_private_methods(self) -> None:
        # If there are any private methods to test, you can access them using the _ClassName__methodname pattern
        pass  # Add specific private method tests here if needed


if __name__ == "__main__":
    unittest.main()
