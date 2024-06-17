import unittest
from unittest.mock import Mock, patch, PropertyMock
from datetime import datetime, timedelta

from caribou.syncers.components.workflow_run_sample import WorkflowRunSample
from caribou.syncers.components.execution_data import ExecutionData
from caribou.syncers.components.transmission_data import TransmissionData


class TestWorkflowRunSample(unittest.TestCase):
    def setUp(self):
        self.run_id = "test_run"
        self.sample = WorkflowRunSample(self.run_id)

    def test_init(self):
        self.assertEqual(self.sample.run_id, self.run_id)
        self.assertEqual(self.sample.request_ids, set())
        self.assertEqual(self.sample.encountered_instance_request_ids, {})
        self.assertIsNone(self.sample.log_start_time)
        self.assertIsNone(self.sample.log_end_time)
        self.assertEqual(self.sample.execution_data, {})
        self.assertEqual(self.sample.transmission_data, {})
        self.assertEqual(self.sample.start_hop_latency, 0.0)
        self.assertEqual(self.sample.start_hop_data_transfer_size, 0.0)
        self.assertIsNone(self.sample.start_hop_instance_name)
        self.assertIsNone(self.sample.start_hop_destination)
        self.assertIsNone(self.sample.start_hop_wpd_data_size)
        self.assertIsNone(self.sample.start_hop_wpd_consumed_read_capacity)
        self.assertEqual(self.sample.cpu_models, set())

    def test_duration(self):
        self.sample.log_start_time = datetime(2024, 1, 1, 10, 0, 0)
        self.sample.log_end_time = datetime(2024, 1, 1, 11, 0, 0)
        self.assertEqual(self.sample.duration, timedelta(hours=1))

    def test_duration_raises_value_error(self):
        with self.assertRaises(ValueError):
            _ = self.sample.duration

    def test_update_log_end_time(self):
        time1 = datetime(2024, 1, 1, 10, 0, 0)
        time2 = datetime(2024, 1, 1, 12, 0, 0)
        self.sample.update_log_end_time(time1)
        self.assertEqual(self.sample.log_end_time, time1)
        self.sample.update_log_end_time(time2)
        self.assertEqual(self.sample.log_end_time, time2)

    def test_get_transmission_data(self):
        taint = "test_taint"
        transmission_data = self.sample.get_transmission_data(taint)
        self.assertIn(taint, self.sample.transmission_data)
        self.assertIsInstance(transmission_data, TransmissionData)

    def test_get_execution_data(self):
        instance_name = "test_instance"
        request_id = "request_1"
        execution_data = self.sample.get_execution_data(instance_name, request_id)
        self.assertIn(instance_name, self.sample.execution_data)
        self.assertIsInstance(execution_data, ExecutionData)
        self.assertIn(request_id, self.sample.encountered_instance_request_ids[instance_name])

    def test_is_valid_and_complete(self):
        self.sample.log_start_time = datetime(2024, 1, 1, 10, 0, 0)
        self.sample.log_end_time = datetime(2024, 1, 1, 11, 0, 0)
        with patch.object(ExecutionData, "is_completed", new_callable=PropertyMock) as mock_completed:
            mock_completed.return_value = True
            self.assertTrue(self.sample.is_valid_and_complete())

    def test_has_duplicate_instances(self):
        self.sample.encountered_instance_request_ids = {"instance_1": {"request_1", "request_2"}}
        self.assertTrue(self.sample._has_duplicate_instances())

    def test_has_no_duplicate_instances(self):
        self.sample.encountered_instance_request_ids = {"instance_1": {"request_1"}}
        self.assertFalse(self.sample._has_duplicate_instances())

    def test_has_incomplete_execution_data(self):
        instance_name = "test_instance"
        execution_data = Mock(spec=ExecutionData)
        type(execution_data).is_completed = PropertyMock(return_value=False)
        self.sample.execution_data[instance_name] = execution_data
        self.assertTrue(self.sample._has_incomplete_execution_data())

    def test_get_formatted_execution_data(self):
        execution_data = Mock(spec=ExecutionData)
        type(execution_data).is_completed = PropertyMock(return_value=True)
        execution_data.to_dict.return_value = {"key": "value"}
        self.sample.execution_data["test_instance"] = execution_data
        result = self.sample._get_formatted_execution_data()
        self.assertEqual(result, [{"key": "value"}])

    def test_get_formatted_invocation_transmission_data(self):
        transmission_data = Mock(spec=TransmissionData)
        type(transmission_data).is_completed = PropertyMock(return_value=True)
        transmission_data.to_dict.return_value = {"key": "value"}
        self.sample.transmission_data["test_taint"] = transmission_data
        result = self.sample._get_formatted_invocation_transmission_data()
        self.assertEqual(result, [{"key": "value"}])

    def test_to_dict(self):
        self.sample.log_start_time = datetime(2024, 1, 1, 10, 0, 0)
        self.sample.log_end_time = datetime(2024, 1, 1, 11, 0, 0)
        with patch.object(WorkflowRunSample, "duration", new_callable=PropertyMock) as mock_duration:
            mock_duration.return_value = timedelta(hours=1)
            expected_dict = {
                "run_id": "test_run",
                "start_time": "2024-01-01 10:00:00,000000",
                "runtime_s": 3600.0,
                "execution_data": [],
                "transmission_data": [],
                "start_hop_info": {
                    "destination": None,
                    "data_transfer_size_gb": 0.0,
                    "latency_s": 0.0,
                    "workflow_placement_decision": {"data_size_gb": None, "consumed_read_capacity": None},
                },
                "unique_cpu_models": [],
            }
            self.assertEqual(self.sample.to_dict()[1], expected_dict)

    @patch("caribou.syncers.components.transmission_data.TransmissionData")
    def test_workflow_run_sample(self, MockTransmissionData):
        self.maxDiff = None

        # Mock TransmissionData instance
        mock_transmission_data = MockTransmissionData.return_value
        mock_transmission_data.transmission_start_time = datetime(2022, 1, 1, 0, 0, 0)
        mock_transmission_data.transmission_end_time = datetime(2022, 1, 1, 1, 0, 0)
        mock_transmission_data.transmission_size = 1.0
        mock_transmission_data.from_instance = "instance1"
        mock_transmission_data.to_instance = "instance2"
        mock_transmission_data.from_region = "provider1:region1"
        mock_transmission_data.to_region = "provider2:region2"
        mock_transmission_data.is_completed = True
        mock_transmission_data.to_dict.return_value = {
            "transmission_size": 1.0,
            "transmission_latency": 3600.0,
            "from_instance": "instance1",
            "to_instance": "instance2",
            "from_region": "provider1:region1",
            "to_region": "provider2:region2",
        }

        # Create a WorkflowRunSample instance
        sample = WorkflowRunSample("run1")

        # Set some attributes
        sample.log_start_time = datetime(2022, 1, 1, 0, 0, 0)
        sample.log_end_time = datetime(2022, 1, 1, 1, 0, 0)
        sample.transmission_data = {"taint1": mock_transmission_data}
        sample.start_hop_latency = 0.2
        sample.start_hop_data_transfer_size = 1.0
        sample.start_hop_destination = "provider1:region1"

        # Check the duration property
        self.assertEqual(sample.duration, timedelta(hours=1))

        # Check the update_log_end_time method
        sample.update_log_end_time(datetime(2022, 1, 1, 2, 0, 0))
        self.assertEqual(sample.log_end_time, datetime(2022, 1, 1, 2, 0, 0))

        # Check the get_transmission_data method
        self.assertEqual(sample.get_transmission_data("taint1"), mock_transmission_data)

        # Check the is_valid_and_complete method
        with patch.object(ExecutionData, "is_completed", new_callable=PropertyMock) as mock_completed:
            mock_completed.return_value = True
            self.assertTrue(sample.is_valid_and_complete())

        # Check the to_dict method
        expected_result = (
            datetime(2022, 1, 1, 0, 0),
            {
                "run_id": "run1",
                "start_time": "2022-01-01 00:00:00,000000",
                "runtime_s": 7200.0,
                "execution_data": [],
                "transmission_data": [
                    {
                        "transmission_size": 1.0,
                        "transmission_latency": 3600.0,
                        "from_instance": "instance1",
                        "to_instance": "instance2",
                        "from_region": "provider1:region1",
                        "to_region": "provider2:region2",
                    }
                ],
                "start_hop_info": {
                    "destination": "provider1:region1",
                    "data_transfer_size_gb": 1.0,
                    "latency_s": 0.2,
                    "workflow_placement_decision": {"data_size_gb": None, "consumed_read_capacity": None},
                },
                "unique_cpu_models": [],
            },
        )
        self.assertEqual(sample.to_dict(), expected_result)


if __name__ == "__main__":
    unittest.main()
