import unittest
from unittest.mock import MagicMock
from datetime import datetime, timedelta
from caribou.syncers.components.execution_data import ExecutionData
from caribou.syncers.components.start_hop_data import StartHopData
from caribou.syncers.components.transmission_data import TransmissionData
from caribou.syncers.components.workflow_run_sample import WorkflowRunSample
from caribou.common.constants import TIME_FORMAT


class TestWorkflowRunSample(unittest.TestCase):
    def setUp(self):
        self.run_id = "test_run_id"
        self.workflow_run_sample = WorkflowRunSample(self.run_id)

    def test_init(self):
        self.assertEqual(self.workflow_run_sample.run_id, self.run_id)
        self.assertEqual(self.workflow_run_sample.request_ids, set())
        self.assertEqual(self.workflow_run_sample.encountered_instance_request_ids, {})
        self.assertIsNone(self.workflow_run_sample.log_start_time)
        self.assertIsNone(self.workflow_run_sample.log_end_time)
        self.assertEqual(self.workflow_run_sample.execution_data, {})
        self.assertEqual(self.workflow_run_sample.transmission_data, {})
        self.assertIsInstance(self.workflow_run_sample.start_hop_data, StartHopData)
        self.assertEqual(self.workflow_run_sample.cpu_models, set())

    def test_duration_without_log_times(self):
        with self.assertRaises(ValueError) as context:
            _ = self.workflow_run_sample.duration
        self.assertIn("log_end_time or log_start_time is not set", str(context.exception))

    def test_duration_with_log_times(self):
        self.workflow_run_sample.log_start_time = datetime(2024, 8, 1, 10, 0, 0)
        self.workflow_run_sample.log_end_time = datetime(2024, 8, 1, 12, 0, 0)
        self.assertEqual(self.workflow_run_sample.duration, timedelta(hours=2))

    def test_update_log_end_time(self):
        new_time = datetime(2024, 8, 1, 13, 0, 0)
        self.workflow_run_sample.update_log_end_time(new_time)
        self.assertEqual(self.workflow_run_sample.log_end_time, new_time)

        earlier_time = datetime(2024, 8, 1, 11, 0, 0)
        self.workflow_run_sample.update_log_end_time(earlier_time)
        self.assertEqual(self.workflow_run_sample.log_end_time, new_time)  # Should not update to earlier time

    def test_get_transmission_data_new_taint(self):
        taint = "test_taint"
        transmission_data = self.workflow_run_sample.get_transmission_data(taint)
        self.assertIsInstance(transmission_data, TransmissionData)
        self.assertIn(taint, self.workflow_run_sample.transmission_data)
        self.assertEqual(self.workflow_run_sample.transmission_data[taint], transmission_data)

    def test_get_transmission_data_existing_taint(self):
        taint = "test_taint"
        existing_transmission_data = TransmissionData(taint)
        self.workflow_run_sample.transmission_data[taint] = existing_transmission_data

        retrieved_transmission_data = self.workflow_run_sample.get_transmission_data(taint)
        self.assertIs(retrieved_transmission_data, existing_transmission_data)

    def test_get_execution_data_new_instance(self):
        instance_name = "test_instance"
        request_id = "test_request_id"

        execution_data = self.workflow_run_sample.get_execution_data(instance_name, request_id)
        self.assertIsInstance(execution_data, ExecutionData)
        self.assertIn(instance_name, self.workflow_run_sample.execution_data)
        self.assertEqual(self.workflow_run_sample.execution_data[instance_name], execution_data)
        self.assertIn(request_id, self.workflow_run_sample.encountered_instance_request_ids[instance_name])

    def test_get_execution_data_existing_instance(self):
        instance_name = "test_instance"
        request_id = "test_request_id"
        existing_execution_data = ExecutionData(instance_name)
        self.workflow_run_sample.execution_data[instance_name] = existing_execution_data

        retrieved_execution_data = self.workflow_run_sample.get_execution_data(instance_name, request_id)
        self.assertIs(retrieved_execution_data, existing_execution_data)
        self.assertIn(request_id, self.workflow_run_sample.encountered_instance_request_ids[instance_name])

    def test_is_valid_and_complete_with_incomplete_data(self):
        self.assertFalse(self.workflow_run_sample.is_valid_and_complete())

    def test_is_valid_and_complete_with_complete_data(self):
        self.workflow_run_sample.log_start_time = datetime(2024, 8, 1, 10, 0, 0)
        self.workflow_run_sample.log_end_time = datetime(2024, 8, 1, 12, 0, 0)

        # Mock the start_hop_data to simulate a completed start hop
        self.workflow_run_sample.start_hop_data = MagicMock()
        self.workflow_run_sample.start_hop_data.is_completed = True

        # Create a mock for execution data that returns True for is_completed
        execution_data_mock = MagicMock()
        execution_data_mock.is_completed = True
        self.workflow_run_sample.execution_data["test_instance"] = execution_data_mock

        self.assertTrue(self.workflow_run_sample.is_valid_and_complete())

    def test_has_duplicate_instances(self):
        instance_name = "test_instance"
        request_id_1 = "request_id_1"
        request_id_2 = "request_id_2"

        self.workflow_run_sample.encountered_instance_request_ids[instance_name] = {request_id_1, request_id_2}
        self.assertTrue(self.workflow_run_sample._has_duplicate_instances())

        self.workflow_run_sample.encountered_instance_request_ids[instance_name] = {request_id_1}
        self.assertFalse(self.workflow_run_sample._has_duplicate_instances())

    def test_has_incomplete_execution_data(self):
        execution_data_mock_1 = MagicMock()
        execution_data_mock_1.is_completed = True
        execution_data_mock_2 = MagicMock()
        execution_data_mock_2.is_completed = False

        self.workflow_run_sample.execution_data["instance_1"] = execution_data_mock_1
        self.workflow_run_sample.execution_data["instance_2"] = execution_data_mock_2

        self.assertTrue(self.workflow_run_sample._has_incomplete_execution_data())

        execution_data_mock_2.is_completed = True
        self.assertFalse(self.workflow_run_sample._has_incomplete_execution_data())

    def test_get_formatted_execution_data(self):
        execution_data_mock = MagicMock()
        execution_data_mock.is_completed = True
        execution_data_mock.to_dict.return_value = {"instance_name": "test_instance"}

        self.workflow_run_sample.execution_data["test_instance"] = execution_data_mock
        formatted_data = self.workflow_run_sample._get_formatted_execution_data()
        self.assertEqual(formatted_data, [{"instance_name": "test_instance"}])

    def test_get_formatted_invocation_transmission_data(self):
        transmission_data_mock = MagicMock()
        transmission_data_mock.is_completed = True
        transmission_data_mock.to_dict.return_value = {"taint": "test_taint"}

        self.workflow_run_sample.transmission_data["test_taint"] = transmission_data_mock
        formatted_data = self.workflow_run_sample._get_formatted_invocation_transmission_data()
        self.assertEqual(formatted_data, [{"taint": "test_taint"}])

    def test_to_dict_without_log_start_time(self):
        with self.assertRaises(ValueError) as context:
            _ = self.workflow_run_sample.to_dict()
        self.assertIn("log_start_time is not set", str(context.exception))

    def test_to_dict_with_complete_data(self):
        self.workflow_run_sample.log_start_time = datetime(2024, 8, 1, 10, 0, 0)
        self.workflow_run_sample.log_end_time = datetime(2024, 8, 1, 12, 0, 0)
        self.workflow_run_sample.cpu_models = {"model1"}

        # Mock execution data
        execution_data_mock = MagicMock()
        execution_data_mock.is_completed = True
        execution_data_mock.to_dict.return_value = {"instance_name": "test_instance"}
        self.workflow_run_sample.execution_data["test_instance"] = execution_data_mock

        # Mock transmission data
        transmission_data_mock = MagicMock()
        transmission_data_mock.is_completed = True
        transmission_data_mock.to_dict.return_value = {"taint": "test_taint"}
        self.workflow_run_sample.transmission_data["test_taint"] = transmission_data_mock

        # Mock start_hop_data to_dict method
        self.workflow_run_sample.start_hop_data = MagicMock()
        self.workflow_run_sample.start_hop_data.to_dict.return_value = {"hop": "start"}

        expected_output = (
            self.workflow_run_sample.log_start_time,
            {
                "run_id": self.run_id,
                "start_time": self.workflow_run_sample.log_start_time.strftime(TIME_FORMAT),
                "runtime_s": 7200.0,  # 2 hours in seconds
                "execution_data": [{"instance_name": "test_instance"}],
                "transmission_data": [{"taint": "test_taint"}],
                "start_hop_info": {"hop": "start"},
                "unique_cpu_models": ["model1"],
            },
        )

        self.assertEqual(self.workflow_run_sample.to_dict(), expected_output)


if __name__ == "__main__":
    unittest.main()
