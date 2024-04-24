import unittest
from datetime import datetime, timedelta
from caribou.syncers.workflow_run_sample import WorkflowRunSample
from caribou.syncers.transmission_data import TransmissionData


class TestWorkflowRunSample(unittest.TestCase):
    def test_workflow_run_sample(self):
        self.maxDiff = None
        # Create a WorkflowRunSample instance
        sample = WorkflowRunSample("run1")

        # Set some attributes
        transmission_data = TransmissionData("taint1")
        transmission_data.transmission_start_time = datetime(2022, 1, 1, 0, 0, 0)
        transmission_data.transmission_end_time = datetime(2022, 1, 1, 1, 0, 0)
        transmission_data.transmission_size = 1.0
        transmission_data.from_instance = "instance1"
        transmission_data.to_instance = "instance2"
        transmission_data.from_region = {"provider": "provider1", "region": "region1"}
        transmission_data.to_region = {"provider": "provider2", "region": "region2"}
        sample.log_start_time = datetime(2022, 1, 1, 0, 0, 0)
        sample.log_end_time = datetime(2022, 1, 1, 1, 0, 0)
        sample.execution_latencies = {"instance1": 0.1}
        sample.transmission_data = {"taint1": transmission_data}
        sample.start_hop_latency = 0.2
        sample.start_hop_data_transfer_size = 1.0
        sample.start_hop_destination = {"provider": "provider1", "region": "region1"}
        sample.non_executions = {"instance1": {"instance2": 1}}

        # Check the duration property
        self.assertEqual(sample.duration, timedelta(hours=1))

        # Check the update_log_end_time method
        sample.update_log_end_time(datetime(2022, 1, 1, 2, 0, 0))
        self.assertEqual(sample.log_end_time, datetime(2022, 1, 1, 2, 0, 0))

        # Check the get_transmission_data method
        self.assertEqual(sample.get_transmission_data("taint1"), transmission_data)

        # Check the is_complete method
        self.assertTrue(sample.is_complete())

        # Check the to_dict method
        expected_result = (
            datetime(2022, 1, 1, 0, 0),
            {
                "run_id": "run1",
                "runtime": 7200.0,
                "start_time": "2022-01-01 00:00:00,000000",
                "execution_latencies": {"instance1": 0.1},
                "transmission_data": [
                    {
                        "transmission_size": 1.0,
                        "transmission_latency": 3600.0,
                        "from_instance": "instance1",
                        "to_instance": "instance2",
                        "from_region": {"provider": "provider1", "region": "region1"},
                        "to_region": {"provider": "provider2", "region": "region2"},
                    }
                ],
                "start_hop_latency": 0.2,
                "start_hop_data_transfer_size": 1.0,
                "start_hop_destination": {"provider": "provider1", "region": "region1"},
                "non_executions": {"instance1": {"instance2": 1}},
            },
        )
        self.assertEqual(sample.to_dict(), expected_result)


if __name__ == "__main__":
    unittest.main()
