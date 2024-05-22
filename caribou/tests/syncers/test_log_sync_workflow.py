import unittest
from unittest.mock import Mock, patch, call
from datetime import datetime
import json
from datetime import timedelta
from caribou.syncers.log_sync_workflow import LogSyncWorkflow
from caribou.common.models.remote_client.remote_client import RemoteClient
from caribou.common.models.remote_client.remote_client_factory import RemoteClientFactory
from caribou.syncers.workflow_run_sample import WorkflowRunSample
from caribou.common.constants import (
    WORKFLOW_SUMMARY_TABLE,
    TIME_FORMAT,
    LOG_VERSION,
    TIME_FORMAT_DAYS,
    FORGETTING_TIME_DAYS,
    GLOBAL_TIME_ZONE,
)


class TestLogSyncWorkflow(unittest.TestCase):
    def setUp(self):
        workflow_id = "test_workflow_id"
        region_clients = {("region1", "client1"): Mock(spec=RemoteClient)}
        deployment_manager_config_str = '{"key": "value"}'
        time_intervals_to_sync = [(datetime.now(GLOBAL_TIME_ZONE), datetime.now(GLOBAL_TIME_ZONE))]
        workflow_summary_client = Mock(spec=RemoteClient)
        previous_data = {"key": "value"}

        self.log_sync_workflow = LogSyncWorkflow(
            workflow_id,
            region_clients,
            deployment_manager_config_str,
            time_intervals_to_sync,
            workflow_summary_client,
            previous_data,
        )

    @patch("caribou.syncers.log_sync_workflow.RemoteClientFactory.get_remote_client")
    def test_get_remote_client(self, get_remote_client_mock):
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
        # Set up the return value for _prepare_data_for_upload
        prepare_data_for_upload_mock.return_value = "test_data"

        # Call the method
        self.log_sync_workflow.sync_workflow()

        # Check that the mocks were called in the correct order with the correct arguments
        sync_logs_mock.assert_called_once()
        prepare_data_for_upload_mock.assert_called_once_with(self.log_sync_workflow._previous_data)
        upload_data_mock.assert_called_once_with("test_data")

    def test_upload_data(self):
        mock_remote_client = Mock(spec=RemoteClient)
        mock_remote_client.update_value_in_table = Mock()
        self.log_sync_workflow._workflow_summary_client = mock_remote_client

        # Set up the test data
        data_for_upload = "test_data"

        # Call the method
        self.log_sync_workflow._upload_data(data_for_upload)

        # Check that update_value_in_table was called with the correct arguments
        mock_remote_client.update_value_in_table.assert_called_once_with(
            WORKFLOW_SUMMARY_TABLE, self.log_sync_workflow.workflow_id, data_for_upload
        )

    @patch.object(LogSyncWorkflow, "_process_logs_for_instance_for_one_region")
    @patch.object(LogSyncWorkflow, "_check_to_forget")
    def test_sync_logs(self, check_to_forget_mock, process_logs_for_instance_for_one_region_mock):
        # Set up the test data
        self.log_sync_workflow._deployed_regions = {
            "function1": {"deploy_region": "region1"},
            "function2": {"deploy_region": "region2"},
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
                "region1",
                self.log_sync_workflow._time_intervals_to_sync[0][0],
                self.log_sync_workflow._time_intervals_to_sync[0][1],
            ),
            call(
                "function2",
                "region2",
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
        # Set up the test data
        functions_instance = "test_instance"
        provider_region = {"provider": "test_provider", "region": "test_region"}
        time_from = datetime.now(GLOBAL_TIME_ZONE)
        time_to = datetime.now(GLOBAL_TIME_ZONE)

        # Set up the return value for _get_remote_client
        mock_remote_client = Mock()
        mock_remote_client.get_logs_between.return_value = ["log1", "log2"]
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
            functions_instance, time_from - timedelta(minutes=30), time_to + timedelta(minutes=30)
        )
        calls = [call("log1", provider_region), call("log2", provider_region)]
        process_log_entry_mock.assert_has_calls(calls)
        setup_lambda_insights_mock.assert_called_once_with(["insight_log1"])

    def test_process_lambda_insights(self):
        # Set up the test data
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
                "duration": 100,
                "cold_start": True,
                "memory_utilization": 50,
                "total_network": 10,
                "cpu_total_time": 5,
            },
            "2": {
                "duration": 200,
                "cold_start": False,
                "memory_utilization": 60,
                "total_network": 20,
                "cpu_total_time": 10,
            },
        }
        self.assertEqual(self.log_sync_workflow._insights_logs, expected_result)

    @patch.object(LogSyncWorkflow, "_extract_from_string")
    @patch.object(LogSyncWorkflow, "_check_to_forget")
    @patch.object(LogSyncWorkflow, "_handle_system_log_messages")
    def test_process_log_entry(self, handle_system_log_messages_mock, check_to_forget_mock, extract_from_string_mock):
        # Set up the test data
        log_entry = (
            f"REPORT [INFO]\ttest_request_id\ttest_run_id\t LOG_VERSION ({LOG_VERSION}) test_log_entry Init Duration"
        )
        provider_region = {"provider": "test_provider", "region": "test_region"}

        # Set up the return values for _extract_from_string
        extract_from_string_mock.side_effect = ["test_request_id", "test_run_id", "2021-01-01 00:00:00,000+00:00"]

        # Call the method
        self.log_sync_workflow._process_log_entry(log_entry, provider_region)

        calls = [
            call(log_entry, r"RequestId: (.*?)\t"),
            call(log_entry, r"RUN_ID \((.*?)\)"),
            call(log_entry, r"TIME \((.*?)\)"),
        ]
        extract_from_string_mock.assert_has_calls(calls)
        handle_system_log_messages_mock.assert_called_once_with(
            log_entry,
            self.log_sync_workflow._collected_logs["test_run_id"],
            provider_region,
            datetime.strptime("2021-01-01 00:00:00,000+00:00", TIME_FORMAT),
            "test_run_id",
        )

    @patch.object(LogSyncWorkflow, "_extract_entry_point_log")
    @patch.object(LogSyncWorkflow, "_extract_invoked_logs")
    @patch.object(LogSyncWorkflow, "_extract_executed_logs")
    @patch.object(LogSyncWorkflow, "_extract_invoking_successor_logs")
    @patch.object(LogSyncWorkflow, "_extract_conditional_non_execution_logs")
    def test_handle_system_log_messages(
        self,
        extract_conditional_non_execution_logs_mock,
        extract_invoking_successor_logs_mock,
        extract_executed_logs_mock,
        extract_invoked_logs_mock,
        extract_entry_point_log_mock,
    ):
        # Set up the test data
        log_entry = "ENTRY_POINT INVOKED EXECUTED INVOKING_SUCCESSOR CONDITIONAL_NON_EXECUTION"
        workflow_run_sample = Mock(spec=WorkflowRunSample)
        provider_region = {"provider": "test_provider", "region": "test_region"}
        log_time = datetime.now(GLOBAL_TIME_ZONE)

        # Call the method
        self.log_sync_workflow._handle_system_log_messages(
            log_entry, workflow_run_sample, provider_region, log_time, "test_run_id"
        )

        # Check that the mocks were called with the correct arguments
        extract_entry_point_log_mock.assert_called_once_with(workflow_run_sample, log_entry, provider_region, log_time)
        extract_invoked_logs_mock.assert_called_once_with(workflow_run_sample, log_entry, provider_region, log_time)
        extract_executed_logs_mock.assert_called_once_with(
            workflow_run_sample, log_entry, {"provider": "test_provider", "region": "test_region"}, "test_run_id"
        )
        extract_invoking_successor_logs_mock.assert_called_once_with(
            workflow_run_sample, log_entry, provider_region, log_time
        )
        extract_conditional_non_execution_logs_mock.assert_called_once_with(workflow_run_sample, log_entry)

    def test_check_to_forget(self):
        # Set up the test data
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

    def test_extract_from_string(self):
        # Set up the test data
        log_entry = "RequestId: test_request_id\t"
        regex = r"RequestId: (.*?)\t"

        # Call the method
        result = self.log_sync_workflow._extract_from_string(log_entry, regex)

        # Check that the result is as expected
        self.assertEqual(result, "test_request_id")

    def test_extract_from_string_no_match(self):
        # Set up the test data
        log_entry = "RequestId: test_request_id\t"
        regex = r"RUN_ID: (.*?)\t"

        # Call the method
        result = self.log_sync_workflow._extract_from_string(log_entry, regex)

        # Check that the result is None because there is no match
        self.assertIsNone(result)

    @patch.object(LogSyncWorkflow, "_extract_from_string")
    def test_extract_entry_point_log(self, extract_from_string_mock):
        # Set up the test data
        workflow_run_sample = WorkflowRunSample("test_run_id")
        log_entry = "PAYLOAD_SIZE (1234) INIT_LATENCY (5678)"
        provider_region = {"provider": "test_provider", "region": "test_region"}
        log_time = datetime.now(GLOBAL_TIME_ZONE)

        # Set up the return values for _extract_from_string
        extract_from_string_mock.side_effect = ["1234", "5678"]

        # Call the method
        self.log_sync_workflow._extract_entry_point_log(workflow_run_sample, log_entry, provider_region, log_time)

        # Check that the WorkflowRunSample object was updated as expected
        self.assertEqual(workflow_run_sample.log_start_time, log_time)
        self.assertEqual(workflow_run_sample.start_hop_data_transfer_size, 1234.0)
        self.assertEqual(workflow_run_sample.start_hop_latency, 5678.0)
        self.assertEqual(workflow_run_sample.start_hop_destination, provider_region)

        # Check that _extract_from_string was called with the correct arguments
        extract_from_string_mock.assert_any_call(log_entry, r"PAYLOAD_SIZE \((.*?)\)")
        extract_from_string_mock.assert_any_call(log_entry, r"INIT_LATENCY \((.*?)\)")

    @patch.object(LogSyncWorkflow, "_extract_from_string")
    def test_extract_invoked_logs(self, extract_from_string_mock):
        # Set up the test data
        workflow_run_sample = WorkflowRunSample("test_run_id")
        log_entry = "TAINT (test_taint)"
        provider_region = {"provider": "test_provider", "region": "test_region"}
        log_time = datetime.now(GLOBAL_TIME_ZONE)

        # Set up the return value for _extract_from_string
        extract_from_string_mock.return_value = "test_taint"

        # Call the method
        self.log_sync_workflow._extract_invoked_logs(workflow_run_sample, log_entry, provider_region, log_time)

        # Check that the WorkflowRunSample object was updated as expected
        transmission_data = workflow_run_sample.get_transmission_data("test_taint")
        self.assertEqual(transmission_data.to_region, provider_region)
        self.assertEqual(transmission_data.transmission_end_time, log_time)

        # Check that _extract_from_string was called with the correct arguments
        extract_from_string_mock.assert_called_once_with(log_entry, r"TAINT \((.*?)\)")

    @patch.object(LogSyncWorkflow, "_extract_from_string")
    def test_extract_executed_logs(self, extract_from_string_mock):
        # Set up the test data
        workflow_run_sample = WorkflowRunSample("test_run_id")
        log_entry = "INSTANCE (test_instance) EXECUTION_TIME (1234)"

        # Set up the return values for _extract_from_string
        extract_from_string_mock.side_effect = ["test_instance", "1234"]

        self.log_sync_workflow._insights_logs = {
            "test_request_id": {
                "duration": 1234,
                "cold_start": False,
                "memory_utilization": 0,
                "total_network": 0,
                "cpu_total_time": 0,
            }
        }

        # Call the method
        self.log_sync_workflow._extract_executed_logs(
            workflow_run_sample, log_entry, {"provider": "test_provider", "region": "test_region"}, "test_request_id"
        )

        self.assertEqual(
            workflow_run_sample.execution_latencies["test_instance"],
            {
                "latency": 1234.0,
                "provider_region": "test_provider:test_region",
                "insights": self.log_sync_workflow._insights_logs["test_request_id"],
            },
        )

        # Check that _extract_from_string was called with the correct arguments
        calls = [call(log_entry, r"INSTANCE \((.*?)\)"), call(log_entry, r"EXECUTION_TIME \((.*?)\)")]
        extract_from_string_mock.assert_has_calls(calls)

    @patch.object(LogSyncWorkflow, "_extract_from_string")
    def test_extract_invoking_successor_logs(self, extract_from_string_mock):
        # Set up the test data
        workflow_run_sample = WorkflowRunSample("test_run_id")
        log_entry = "TAINT (test_taint) INSTANCE (test_instance) SUCCESSOR (test_successor) PAYLOAD_SIZE (1234)"
        provider_region = {"provider": "test_provider", "region": "test_region"}
        log_time = datetime.now(GLOBAL_TIME_ZONE)

        # Set up the return values for _extract_from_string
        extract_from_string_mock.side_effect = ["test_taint", "test_instance", "test_successor", "1234"]

        # Call the method
        self.log_sync_workflow._extract_invoking_successor_logs(
            workflow_run_sample, log_entry, provider_region, log_time
        )

        # Check that the WorkflowRunSample object was updated as expected
        transmission_data = workflow_run_sample.get_transmission_data("test_taint")
        self.assertEqual(transmission_data.from_region, provider_region)
        self.assertEqual(transmission_data.from_instance, "test_instance")
        self.assertEqual(transmission_data.to_instance, "test_successor")
        self.assertEqual(transmission_data.transmission_start_time, log_time)
        self.assertEqual(transmission_data.transmission_size, 1234.0)

        # Check that _extract_from_string was called with the correct arguments
        calls = [
            call(log_entry, r"TAINT \((.*?)\)"),
            call(log_entry, r"INSTANCE \((.*?)\)"),
            call(log_entry, r"SUCCESSOR \((.*?)\)"),
            call(log_entry, r"PAYLOAD_SIZE \((.*?)\)"),
        ]
        extract_from_string_mock.assert_has_calls(calls)

    @patch.object(LogSyncWorkflow, "_extract_from_string")
    def test_extract_conditional_non_execution_logs(self, extract_from_string_mock):
        # Set up the test data
        workflow_run_sample = WorkflowRunSample("test_run_id")
        log_entry = "INSTANCE (test_instance) SUCCESSOR (test_successor)"

        # Set up the return values for _extract_from_string
        extract_from_string_mock.side_effect = ["test_instance", "test_successor"]

        # Call the method
        self.log_sync_workflow._extract_conditional_non_execution_logs(workflow_run_sample, log_entry)

        # Check that the WorkflowRunSample object was updated as expected
        self.assertEqual(workflow_run_sample.non_executions["test_instance"]["test_successor"], 1)

        # Check that _extract_from_string was called with the correct arguments
        calls = [call(log_entry, r"INSTANCE \((.*?)\)"), call(log_entry, r"SUCCESSOR \((.*?)\)")]
        extract_from_string_mock.assert_has_calls(calls)

    @patch.object(LogSyncWorkflow, "_filter_daily_invocation_counts")
    @patch.object(LogSyncWorkflow, "_merge_daily_invocation_counts")
    @patch.object(LogSyncWorkflow, "_format_collected_logs")
    @patch.object(LogSyncWorkflow, "_fill_up_collected_logs")
    def test_prepare_data_for_upload(
        self,
        fill_up_collected_logs_mock,
        format_collected_logs_mock,
        merge_daily_invocation_counts_mock,
        filter_daily_invocation_counts_mock,
    ):
        # Set up the test data
        previous_data = {
            "daily_invocation_counts": {"2022-01-01": 10},
            "logs": [{"runtime": 1.0}],
            "workflow_runtime_samples": [1.0],
            "last_sync_time": "2022-01-01T00:00:00,000+00:00",
        }
        self.log_sync_workflow._time_intervals_to_sync = [
            (datetime.now(GLOBAL_TIME_ZONE) - timedelta(days=1), datetime.now(GLOBAL_TIME_ZONE))
        ]

        # Set up the return value for _format_collected_logs
        format_collected_logs_mock.return_value = [{"runtime": 1.0}]

        # Call the method
        result = self.log_sync_workflow._prepare_data_for_upload(previous_data)

        # Check that the result is as expected
        expected_result = {
            "daily_invocation_counts": {"2022-01-01": 10},
            "logs": [{"runtime": 1.0}],
            "workflow_runtime_samples": [1.0],
            "last_sync_time": self.log_sync_workflow._time_intervals_to_sync[-1][1].strftime(TIME_FORMAT),
        }
        self.assertEqual(json.loads(result), expected_result)

        # Check that the mocks were called with the correct arguments
        filter_daily_invocation_counts_mock.assert_called_once_with(previous_data["daily_invocation_counts"])
        merge_daily_invocation_counts_mock.assert_called_once_with(previous_data["daily_invocation_counts"])
        format_collected_logs_mock.assert_called_once()
        fill_up_collected_logs_mock.assert_called_once_with(format_collected_logs_mock.return_value, previous_data)

    def test_fill_up_collected_logs(self):
        # Set up the test data
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
        # Set up the test data
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

    from unittest.mock import patch

    @patch.object(LogSyncWorkflow, "_extend_existing_execution_instance_region")
    @patch.object(LogSyncWorkflow, "_extend_existing_transmission_from_instance_to_instance_region")
    @patch("caribou.syncers.log_sync_workflow.WorkflowRunSample")
    def test_format_collected_logs(
        self,
        WorkflowRunSampleMock,
        extend_existing_transmission_from_instance_to_instance_region_mock,
        extend_existing_execution_instance_region_mock,
    ):
        # Set up the mock
        WorkflowRunSampleMock.return_value.is_complete.return_value = True
        WorkflowRunSampleMock.return_value.to_dict.return_value = (
            1,
            {
                "start_time": "2022-01-01T00:00:00,000+00:00",
                "end_time": "2022-01-01T00:00:00,000+00:00",
                "execution_latencies": {"function1": {"provider_region": "provider1:region1", "latency": "0.1"}},
                "start_hop_destination": {"provider": "provider1", "region": "region1"},
                "transmission_data": [
                    {
                        "from_instance": "instance1",
                        "to_instance": "instance2",
                        "from_region": {"provider": "provider1", "region": "region1"},
                        "to_region": {"provider": "provider2", "region": "region2"},
                        "transmission_size": 1.0,
                        "transmission_latency": 0.1,
                    }
                ],
                "non_executions": {"instance1": {"instance2": 1}},
            },
        )

        self.log_sync_workflow._collected_logs = {"workflow1": WorkflowRunSampleMock()}

        # Set up the test data
        now = datetime.strptime("2022-01-01 00:00:00,000+00:00", TIME_FORMAT)

        # Call the method
        result = self.log_sync_workflow._format_collected_logs()

        # Check that the result is as expected
        expected_result = [
            {
                "start_time": "2022-01-01T00:00:00,000+00:00",
                "end_time": "2022-01-01T00:00:00,000+00:00",
                "execution_latencies": {"function1": {"provider_region": "provider1:region1", "latency": "0.1"}},
                "start_hop_destination": {"provider": "provider1", "region": "region1"},
                "transmission_data": [
                    {
                        "from_instance": "instance1",
                        "to_instance": "instance2",
                        "from_region": {"provider": "provider1", "region": "region1"},
                        "to_region": {"provider": "provider2", "region": "region2"},
                        "transmission_size": 1.0,
                        "transmission_latency": 0.1,
                    }
                ],
                "non_executions": {"instance1": {"instance2": 1}},
            }
        ]
        self.assertEqual(result, expected_result)

        # Check that the mocks were called with the correct arguments
        extend_existing_execution_instance_region_mock.assert_called_once_with(expected_result[0])
        extend_existing_transmission_from_instance_to_instance_region_mock.assert_called_once_with(expected_result[0])

    def test_filter_daily_invocation_counts(self):
        # Set up the test data
        now = datetime.now(GLOBAL_TIME_ZONE)
        previous_daily_invocation_counts = {
            (now - timedelta(days=i)).strftime(TIME_FORMAT_DAYS): i for i in range(FORGETTING_TIME_DAYS + 2)
        }

        # Call the method
        self.log_sync_workflow._filter_daily_invocation_counts(previous_daily_invocation_counts)

        # Check that the previous_daily_invocation_counts dictionary was updated as expected
        expected_result = {(now - timedelta(days=i)).strftime(TIME_FORMAT_DAYS): i for i in range(FORGETTING_TIME_DAYS)}
        self.assertEqual(previous_daily_invocation_counts, expected_result)

    def test_merge_daily_invocation_counts(self):
        # Set up the test data
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
        # Set up the test data
        previous_log = {
            "execution_latencies": {
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
        # Set up the test data
        log = {
            "execution_latencies": {
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
        # Set up the test data
        log = {
            "transmission_data": [
                {
                    "from_instance": "instance1",
                    "to_instance": "instance2",
                    "from_region": {"provider": "provider1", "region": "region1"},
                    "to_region": {"provider": "provider2", "region": "region2"},
                    "transmission_size": 1.0,
                    "transmission_latency": 0.1,
                },
                {
                    "from_instance": "instance1",
                    "to_instance": "instance2",
                    "from_region": {"provider": "provider1", "region": "region1"},
                    "to_region": {"provider": "provider2", "region": "region3"},
                    "transmission_size": 1.0,
                    "transmission_latency": 0.1,
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
                        "provider1:region1": {"provider2:region2": 6, "provider2:region3": 1},
                    }
                }
            }
        }
        self.assertEqual(self.log_sync_workflow._existing_data, expected_data)

    def test_check_for_missing_transmission_from_instance_to_instance_region(self):
        # Set up the test data
        previous_log = {
            "transmission_data": [
                {
                    "from_instance": "instance1",
                    "to_instance": "instance2",
                    "from_region": {"provider": "provider1", "region": "region1"},
                    "to_region": {"provider": "provider2", "region": "region2"},
                },
                {
                    "from_instance": "instance1",
                    "to_instance": "instance2",
                    "from_region": {"provider": "provider1", "region": "region1"},
                    "to_region": {"provider": "provider2", "region": "region3"},
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

    def test_check_for_missing_execution_instance_region_detailed(self):
        # Set up the test data
        previous_log = {
            "execution_latencies": {
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

    @patch.object(LogSyncWorkflow, "_check_for_missing_execution_instance_region")
    @patch.object(LogSyncWorkflow, "_check_for_missing_transmission_from_instance_to_instance_region")
    def test_selectively_add_previous_logs(
        self,
        check_for_missing_transmission_from_instance_to_instance_region_mock,
        check_for_missing_execution_instance_region_mock,
    ):
        # Set up the test data
        collected_logs = [{"log1": "data1"}]
        previous_log = {"log2": "data2"}

        # Set up the return values for the mocks
        check_for_missing_execution_instance_region_mock.return_value = True
        check_for_missing_transmission_from_instance_to_instance_region_mock.return_value = False

        # Call the method
        self.log_sync_workflow._selectively_add_previous_logs(collected_logs, previous_log)

        # Check that the collected_logs list was updated as expected
        expected_logs = [previous_log, {"log1": "data1"}]
        self.assertEqual(collected_logs, expected_logs)

        # Check that the mocks were called with the correct arguments
        check_for_missing_execution_instance_region_mock.assert_called_once_with(previous_log)
        check_for_missing_transmission_from_instance_to_instance_region_mock.assert_called_once_with(previous_log)


if __name__ == "__main__":
    unittest.main()
