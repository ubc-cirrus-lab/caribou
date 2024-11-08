import unittest
from unittest.mock import Mock, call, patch
from datetime import datetime, timedelta
import json
from caribou.syncers.log_sync_workflow import LogSyncWorkflow
from caribou.common.models.remote_client.remote_client import RemoteClient
from caribou.syncers.components.workflow_run_sample import WorkflowRunSample
from caribou.common.constants import (
    WORKFLOW_SUMMARY_TABLE,
    TIME_FORMAT,
    TIME_FORMAT_DAYS,
    FORGETTING_TIME_DAYS,
    GLOBAL_TIME_ZONE,
    BUFFER_LAMBDA_INSIGHTS_GRACE_PERIOD,
)


class TestLogSyncWorkflow(unittest.TestCase):
    def setUp(self):
        # Initialize LogSyncWorkflow with mock data
        self.workflow_id = "test_workflow_id"
        self.region_clients = {("region1", "client1"): Mock(spec=RemoteClient)}
        self.deployment_manager_config_str = '{"deployed_regions": "{}"}'
        self.time_intervals_to_sync = [(datetime.now(GLOBAL_TIME_ZONE), datetime.now(GLOBAL_TIME_ZONE))]
        self.workflow_summary_client = Mock(spec=RemoteClient)
        self.previous_data = {"key": "value"}

        # Instantiate the LogSyncWorkflow
        self.log_sync_workflow = LogSyncWorkflow(
            self.workflow_id,
            self.region_clients,
            self.deployment_manager_config_str,
            self.time_intervals_to_sync,
            self.workflow_summary_client,
            self.previous_data,
        )

    def test_init(self):
        # Test that initialization sets up attributes correctly
        self.assertEqual(self.log_sync_workflow.workflow_id, self.workflow_id)
        self.assertEqual(self.log_sync_workflow._region_clients, self.region_clients)
        self.assertEqual(self.log_sync_workflow._workflow_summary_client, self.workflow_summary_client)
        self.assertEqual(self.log_sync_workflow._previous_data, self.previous_data)
        self.assertEqual(self.log_sync_workflow._collected_logs, {})
        self.assertEqual(self.log_sync_workflow._deployed_regions, {})

    def test_load_information(self):
        # Test that deployment information is loaded correctly
        config_str = '{"deployed_regions": "{\\"region1\\": \\"client1\\"}"}'
        self.log_sync_workflow._load_information(config_str)
        expected = {"region1": "client1"}
        self.assertEqual(self.log_sync_workflow._deployed_regions, expected)

    @patch("caribou.syncers.log_sync_workflow.RemoteClientFactory.get_remote_client")
    def test_get_remote_client(self, get_remote_client_mock):
        # Test getting a remote client
        get_remote_client_mock.return_value = Mock(spec=RemoteClient)
        provider_region = {"provider": "test_provider", "region": "test_region"}

        # Call the method and get the result
        result = self.log_sync_workflow._get_remote_client(provider_region)

        # Check that the result is a RemoteClient
        self.assertIsInstance(result, RemoteClient)

        # Check that the RemoteClient was added to _region_clients
        self.assertIn(("test_provider", "test_region"), self.log_sync_workflow._region_clients)

        # Check that get_remote_client was called with the correct arguments
        get_remote_client_mock.assert_called_once_with("test_provider", "test_region")

    @patch.object(LogSyncWorkflow, "_sync_logs")
    @patch.object(LogSyncWorkflow, "_prepare_data_for_upload")
    @patch.object(LogSyncWorkflow, "_upload_data")
    def test_sync_workflow(self, upload_data_mock, prepare_data_for_upload_mock, sync_logs_mock):
        # Test the sync_workflow method
        prepare_data_for_upload_mock.return_value = "{}"

        # Call the method
        self.log_sync_workflow.sync_workflow()

        # Check that the mocks were called in the correct order with the correct arguments
        sync_logs_mock.assert_called_once()
        prepare_data_for_upload_mock.assert_called_once_with(self.previous_data)
        upload_data_mock.assert_called_once_with("{}")

    def test_upload_data(self):
        # Test the _upload_data method
        data_for_upload = "test_data"
        self.log_sync_workflow._upload_data(data_for_upload)

        # Check that update_value_in_table was called with the correct arguments
        self.workflow_summary_client.update_value_in_table.assert_called_once_with(
            WORKFLOW_SUMMARY_TABLE,
            self.workflow_id,
            data_for_upload,
            convert_to_bytes=True,
        )

    @patch.object(LogSyncWorkflow, "_process_logs_for_instance_for_one_region")
    @patch.object(LogSyncWorkflow, "_check_to_forget")
    def test_sync_logs(self, check_to_forget_mock, process_logs_for_instance_for_one_region_mock):
        # Test the _sync_logs method
        self.log_sync_workflow._deployed_regions = {
            "function1": {"deploy_region": {"provider": "aws", "region": "us-east-1"}},
            "function2": {"deploy_region": {"provider": "aws", "region": "us-east-2"}},
        }
        self.log_sync_workflow._time_intervals_to_sync = [
            (datetime.now(GLOBAL_TIME_ZONE), datetime.now(GLOBAL_TIME_ZONE))
        ]

        # Call the method
        self.log_sync_workflow._sync_logs()

        # Check that the mocks were called with the correct arguments
        calls = [
            call(
                "function1",
                {"provider": "aws", "region": "us-east-1"},
                self.log_sync_workflow._time_intervals_to_sync[0][0],
                self.log_sync_workflow._time_intervals_to_sync[0][1],
            ),
            call(
                "function2",
                {"provider": "aws", "region": "us-east-2"},
                self.log_sync_workflow._time_intervals_to_sync[0][0],
                self.log_sync_workflow._time_intervals_to_sync[0][1],
            ),
        ]
        process_logs_for_instance_for_one_region_mock.assert_has_calls(calls)
        check_to_forget_mock.assert_called_once()

    @patch.object(LogSyncWorkflow, "_get_remote_client")
    @patch.object(LogSyncWorkflow, "_process_log_entry")
    @patch.object(LogSyncWorkflow, "_setup_lambda_insights")
    def test_process_logs_for_instance_for_one_region(
        self, setup_lambda_insights_mock, process_log_entry_mock, get_remote_client_mock
    ):
        # Test processing logs for one region
        functions_instance = "test_instance"
        provider_region = {"provider": "test_provider", "region": "test_region"}
        time_from = datetime.now(GLOBAL_TIME_ZONE)
        time_to = datetime.now(GLOBAL_TIME_ZONE)

        # Set up the return value for _get_remote_client
        mock_remote_client = Mock()
        mock_remote_client.get_logs_between.return_value = ["[CARIBOU] log1", "log2"]
        mock_remote_client.get_insights_logs_between.return_value = ["insight_log1"]
        get_remote_client_mock.return_value = mock_remote_client

        # Call the method
        self.log_sync_workflow._process_logs_for_instance_for_one_region(
            functions_instance, provider_region, time_from, time_to
        )

        # Check that the mocks were called with the correct arguments
        get_remote_client_mock.assert_called_once_with(provider_region)
        mock_remote_client.get_logs_between.assert_called_once_with(functions_instance, time_from, time_to)
        mock_remote_client.get_insights_logs_between.assert_called_once_with(
            functions_instance,
            time_from - timedelta(minutes=BUFFER_LAMBDA_INSIGHTS_GRACE_PERIOD),
            time_to + timedelta(minutes=BUFFER_LAMBDA_INSIGHTS_GRACE_PERIOD),
        )
        process_log_entry_mock.assert_any_call("[CARIBOU] log1", provider_region, time_to)
        setup_lambda_insights_mock.assert_called_once_with(["insight_log1"])

    def test_setup_lambda_insights(self):
        # Test setting up lambda insights
        logs = [
            json.dumps(
                {
                    "request_id": "1",
                    "duration": 100,
                    "cold_start": True,
                    "memory_utilization": 50,
                    "total_network": 10,
                    "cpu_total_time": 5,
                }
            ),
            json.dumps(
                {
                    "request_id": "2",
                    "duration": 200,
                    "cold_start": False,
                    "memory_utilization": 60,
                    "total_network": 20,
                    "cpu_total_time": 10,
                }
            ),
        ]

        # Call the method
        self.log_sync_workflow._setup_lambda_insights(logs)

        # Check the _insights_logs attribute
        expected_result = {
            "1": {
                "duration": 0.1,
                "cold_start": True,
                "memory_utilization": 50,
                "total_network": 10,
                "cpu_total_time": 0.005,
            },
            "2": {
                "duration": 0.2,
                "cold_start": False,
                "memory_utilization": 60,
                "total_network": 20,
                "cpu_total_time": 0.01,
            },
        }
        self.assertEqual(self.log_sync_workflow._insights_logs, expected_result)

    def test_process_log_entry(self):
        # Test processing a log entry
        time_to = datetime.now(GLOBAL_TIME_ZONE)
        log_entry = f"[CARIBOU]	2024-08-02T16:43:12.323Z	366b3663-2679-447c-86a0-ea2d8df06bcf	TIME (2024-08-02 16:43:12,323041+0000) RUN_ID (5f627048-fbc7-4a6f-9eee-309de1ea852a) MESSAGE (WPD_OVERRIDE: WPD was overriden by debug_workflow_placement_override OVERRIDING_WORKFLOW_PLACEMENT_SIZE (1.8067657947540283e-06) GB) LOG_VERSION (0.0.4)"
        provider_region = {"provider": "test_provider", "region": "test_region"}

        # Call the method
        self.log_sync_workflow._process_log_entry(log_entry, provider_region, time_to)

        # Check that the workflow_run_sample is created and updated
        run_id = "5f627048-fbc7-4a6f-9eee-309de1ea852a"
        self.assertIn(run_id, self.log_sync_workflow._collected_logs)
        workflow_run_sample = self.log_sync_workflow._collected_logs[run_id]
        self.assertIsInstance(workflow_run_sample, WorkflowRunSample)
        self.assertIn("366b3663-2679-447c-86a0-ea2d8df06bcf", workflow_run_sample.request_ids)

    def test_extract_from_string(self):
        # Test extracting a string from a log entry
        log_entry = "RequestId: test_request_id\t"
        regex = r"RequestId: (.*?)\t"
        result = self.log_sync_workflow._extract_from_string(log_entry, regex)
        self.assertEqual(result, "test_request_id")

    def test_extract_from_string_no_match(self):
        # Test extracting a string with no match
        log_entry = "RequestId: test_request_id\t"
        regex = r"RUN_ID: (.*?)\t"
        result = self.log_sync_workflow._extract_from_string(log_entry, regex)
        self.assertIsNone(result)

    def test_check_to_forget(self):
        # Test the _check_to_forget method
        self.log_sync_workflow._collected_logs = {
            "run1": Mock(spec=WorkflowRunSample, request_ids={"request1"}),
            "run2": Mock(spec=WorkflowRunSample, request_ids={"request2"}),
            "run3": Mock(spec=WorkflowRunSample, request_ids={"request3"}),
        }
        self.log_sync_workflow._tainted_cold_start_samples = {"request2"}
        self.log_sync_workflow._blacklisted_run_ids = set()

        # Call the method
        self.log_sync_workflow._check_to_forget()

        # Check that the tainted run was removed from _collected_logs and added to _blacklisted_run_ids
        self.assertNotIn("run2", self.log_sync_workflow._collected_logs)
        self.assertIn("run2", self.log_sync_workflow._blacklisted_run_ids)

        # Check that _forgetting is False because the size of _collected_logs is not equal to FORGETTING_NUMBER
        self.assertFalse(self.log_sync_workflow._forgetting)

    def test_format_region(self):
        # Test formatting a region string
        region = {"provider": "aws", "region": "us-east-1"}
        result = self.log_sync_workflow._format_region(region)
        self.assertEqual(result, "aws:us-east-1")

    def test_format_region_none(self):
        # Test formatting a region string with None
        result = self.log_sync_workflow._format_region(None)
        self.assertIsNone(result)

    def test_fill_up_collected_logs(self):
        # Test filling up collected logs
        now = datetime.now(GLOBAL_TIME_ZONE)
        collected_logs = [{"start_time": (now - timedelta(days=1)).strftime(TIME_FORMAT)}]
        previous_data = {
            "logs": [
                {"start_time": (now - timedelta(days=5)).strftime(TIME_FORMAT)},
                {"start_time": (now - timedelta(days=4)).strftime(TIME_FORMAT)},
                {"start_time": (now - timedelta(days=3)).strftime(TIME_FORMAT)},
                {"start_time": (now - timedelta(days=2)).strftime(TIME_FORMAT)},
            ]
        }

        # Call the method
        self.log_sync_workflow._fill_up_collected_logs(collected_logs, previous_data)

        # Check that the collected_logs list was updated as expected
        expected_result = [
            {"start_time": (now - timedelta(days=5)).strftime(TIME_FORMAT)},
            {"start_time": (now - timedelta(days=4)).strftime(TIME_FORMAT)},
            {"start_time": (now - timedelta(days=3)).strftime(TIME_FORMAT)},
            {"start_time": (now - timedelta(days=2)).strftime(TIME_FORMAT)},
            {"start_time": (now - timedelta(days=1)).strftime(TIME_FORMAT)},
        ]
        self.assertEqual(collected_logs, expected_result)

    def test_fill_up_collected_logs_already_full(self):
        # Test that collected logs are not updated when they are already full
        now = datetime.now(GLOBAL_TIME_ZONE)

        collected_logs = [{"start_time": (now - timedelta(days=i)).strftime(TIME_FORMAT)} for i in range(2, -1, -1)]
        previous_data = {
            "logs": [{"start_time": (now - timedelta(days=i)).strftime(TIME_FORMAT)} for i in range(3, 2, -1)]
        }
        # Call the method
        self.log_sync_workflow._fill_up_collected_logs(collected_logs, previous_data)

        # Check that the collected_logs list was not updated because it was already full
        expected_result = [{"start_time": (now - timedelta(days=i)).strftime(TIME_FORMAT)} for i in range(3, -1, -1)]
        self.assertEqual(collected_logs, expected_result)

    @patch.object(LogSyncWorkflow, "_extend_existing_execution_instance_region")
    @patch.object(LogSyncWorkflow, "_extend_existing_transmission_from_instance_to_instance_region")
    @patch("caribou.syncers.log_sync_workflow.WorkflowRunSample")
    def test_format_collected_logs(
        self,
        WorkflowRunSampleMock,
        extend_existing_transmission_from_instance_to_instance_region_mock,
        extend_existing_execution_instance_region_mock,
    ):
        # Test formatting collected logs
        WorkflowRunSampleMock.return_value.is_valid_and_complete.return_value = True
        WorkflowRunSampleMock.return_value.to_dict.return_value = (
            "2022-01-01T00:00:00,000+00:00",
            {
                "execution_data": [{"instance_name": "function1", "provider_region": "provider1:region1"}],
                "transmission_data": [
                    {
                        "from_instance": "instance1",
                        "to_instance": "instance2",
                        "from_region": "provider1:region1",
                        "to_region": "provider2:region2",
                        "transmission_size": 1.0,
                    }
                ],
            },
        )

        self.log_sync_workflow._collected_logs = {"workflow1": WorkflowRunSampleMock()}

        # Call the method
        result = self.log_sync_workflow._format_collected_logs()

        # Check that the result is as expected
        expected_result = [
            {
                "execution_data": [{"instance_name": "function1", "provider_region": "provider1:region1"}],
                "transmission_data": [
                    {
                        "from_instance": "instance1",
                        "to_instance": "instance2",
                        "from_region": "provider1:region1",
                        "to_region": "provider2:region2",
                        "transmission_size": 1.0,
                    }
                ],
            }
        ]
        self.assertEqual(result, expected_result)

        # Check that the mocks were called with the correct arguments
        extend_existing_execution_instance_region_mock.assert_called_once_with(expected_result[0])
        extend_existing_transmission_from_instance_to_instance_region_mock.assert_called_once_with(expected_result[0])

    def test_filter_daily_invocation_counts(self):
        # Test filtering daily invocation counts
        now = datetime.now(GLOBAL_TIME_ZONE)
        previous_daily_invocation_counts = {
            (now - timedelta(days=i)).strftime(TIME_FORMAT_DAYS): i for i in range(FORGETTING_TIME_DAYS + 2)
        }

        # Call the method
        self.log_sync_workflow._filter_daily_counts(previous_daily_invocation_counts)

        # Check that the previous_daily_invocation_counts dictionary was updated as expected
        expected_result = {(now - timedelta(days=i)).strftime(TIME_FORMAT_DAYS): i for i in range(FORGETTING_TIME_DAYS)}
        self.assertEqual(previous_daily_invocation_counts, expected_result)

    def test_merge_daily_invocation_counts(self):
        # Test merging daily invocation counts
        now = datetime.now(GLOBAL_TIME_ZONE)
        previous_daily_invocation_counts = {(now - timedelta(days=i)).strftime(TIME_FORMAT_DAYS): i for i in range(5)}
        self.log_sync_workflow._daily_invocation_set = {
            (now - timedelta(days=i)).strftime(TIME_FORMAT_DAYS): set(range(i, i + 5)) for i in range(5)
        }

        # Call the method
        self.log_sync_workflow._merge_daily_invocation_counts(previous_daily_invocation_counts)

        # Check that the previous_daily_invocation_counts dictionary was updated as expected
        expected_result = {(now - timedelta(days=i)).strftime(TIME_FORMAT_DAYS): i + 5 for i in range(5)}
        self.assertEqual(previous_daily_invocation_counts, expected_result)

    def test_check_for_missing_execution_instance_region(self):
        # Test checking for missing execution instance region
        previous_log = {
            "execution_data": {
                "function1": {"provider_region": "provider1:region1"},
                "function2": {"provider_region": "provider1:region2"},
            }
        }
        self.log_sync_workflow._existing_data = {
            "execution_instance_region": {
                "function1": {"provider1:region1": 5},
            }
        }

        # Call the method
        result = self.log_sync_workflow._check_for_missing_execution_instance_region(previous_log)

        # Check that the _existing_data dictionary was updated as expected
        expected_data = {
            "execution_instance_region": {
                "function1": {"provider1:region1": 6},
                "function2": {"provider1:region2": 1},
            }
        }
        self.assertEqual(self.log_sync_workflow._existing_data, expected_data)

        # Check that the method returned the correct value
        self.assertTrue(result)

    def test_extend_existing_execution_instance_region(self):
        # Test extending existing execution instance region
        log = {
            "execution_data": [
                {"instance_name": "function1", "provider_region": "provider1:region1"},
                {"instance_name": "function2", "provider_region": "provider1:region2"},
            ]
        }
        self.log_sync_workflow._existing_data = {
            "execution_instance_region": {
                "function1": {"provider1:region1": 5},
            }
        }

        # Call the method
        self.log_sync_workflow._extend_existing_execution_instance_region(log)

        # Check that the _existing_data dictionary was updated as expected
        expected_data = {
            "execution_instance_region": {
                "function1": {"provider1:region1": 6},
                "function2": {"provider1:region2": 1},
            }
        }
        self.assertEqual(self.log_sync_workflow._existing_data, expected_data)

    def test_extend_existing_transmission_from_instance_to_instance_region(self):
        # Test extending existing transmission from instance to instance region
        log = {
            "transmission_data": [
                {
                    "from_instance": "instance1",
                    "to_instance": "instance2",
                    "from_region": "provider1:region1",
                    "to_region": "provider2:region2",
                    "transmission_size": 1.0,
                    "from_direct_successor": True,
                    "successor_invoked": True,
                },
                {
                    "from_instance": "instance1",
                    "to_instance": "instance2",
                    "from_region": "provider1:region1",
                    "to_region": "provider2:region3",
                    "transmission_size": 2.0,
                    "from_direct_successor": True,
                    "successor_invoked": False,
                },
                {
                    "from_instance": "instance1",
                    "to_instance": "instance2",
                    "from_region": "provider1:region1",
                    "to_region": "provider2:region4",
                    "transmission_size": 5.0,
                    "from_direct_successor": True,
                    "successor_invoked": True,
                },
            ]
        }
        self.log_sync_workflow._existing_data = {
            "transmission_from_instance_to_instance_region": {
                "instance1": {
                    "instance2": {
                        "provider1:region1": {"provider2:region2": 5},
                    }
                }
            }
        }

        # Call the method
        self.log_sync_workflow._extend_existing_transmission_from_instance_to_instance_region(log)

        # Check that the _existing_data dictionary was updated as expected
        expected_data = {
            "transmission_from_instance_to_instance_region": {
                "instance1": {
                    "instance2": {
                        "provider1:region1": {"provider2:region2": 6, "provider2:region3": 0, "provider2:region4": 1},
                    }
                }
            }
        }
        self.assertEqual(self.log_sync_workflow._existing_data, expected_data)

    def test_check_for_missing_transmission_from_instance_to_instance_region(self):
        # Test checking for missing transmission from instance to instance region
        previous_log = {
            "transmission_data": [
                {
                    "from_instance": "instance1",
                    "to_instance": "instance2",
                    "from_region": "provider1:region1",
                    "to_region": "provider2:region2",
                },
                {
                    "from_instance": "instance1",
                    "to_instance": "instance2",
                    "from_region": "provider1:region1",
                    "to_region": "provider2:region3",
                },
            ]
        }
        self.log_sync_workflow._existing_data = {
            "transmission_from_instance_to_instance_region": {
                "instance1": {
                    "instance2": {
                        "provider1:region1": {"provider2:region2": 5},
                    }
                }
            }
        }

        # Call the method
        result = self.log_sync_workflow._check_for_missing_transmission_from_instance_to_instance_region(previous_log)

        # Check that the _existing_data dictionary was updated as expected
        expected_data = {
            "transmission_from_instance_to_instance_region": {
                "instance1": {
                    "instance2": {
                        "provider1:region1": {"provider2:region2": 6, "provider2:region3": 1},
                    }
                }
            }
        }
        self.assertEqual(self.log_sync_workflow._existing_data, expected_data)

        # Check that the method returned the correct value
        self.assertTrue(result)


if __name__ == "__main__":
    unittest.main()
