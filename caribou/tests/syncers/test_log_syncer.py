from unittest.mock import patch, MagicMock
from caribou.syncers.log_syncer import LogSyncer
from caribou.common.constants import (
    DEPLOYMENT_MANAGER_RESOURCE_TABLE,
    WORKFLOW_SUMMARY_TABLE,
    FORGETTING_TIME_DAYS,
    GLOBAL_TIME_ZONE,
    TIME_FORMAT,
)
import unittest
from datetime import datetime, timedelta


class TestLogSyncer(unittest.TestCase):
    @patch("caribou.syncers.log_syncer.Endpoints")
    def test_init(self, mock_endpoints):
        # Set up the mock
        mock_datastore_client = MagicMock()
        mock_endpoints.return_value.get_datastore_client.return_value = mock_datastore_client

        # Create a LogSyncer instance
        log_syncer = LogSyncer()

        # Check that the attributes are set as expected
        self.assertEqual(log_syncer.endpoints, mock_endpoints.return_value)
        self.assertEqual(log_syncer._workflow_summary_client, mock_datastore_client)
        self.assertEqual(log_syncer._region_clients, {})

        # Check that get_datastore_client was called
        mock_endpoints.return_value.get_datastore_client.assert_called_once()
        self.maxDiff = None

    @patch("caribou.syncers.log_syncer.Endpoints")
    @patch("caribou.syncers.log_syncer.LogSyncWorkflow")
    @patch("caribou.syncers.log_syncer.LogSyncer._get_time_intervals_to_sync", return_value=["interval1"])
    def test_sync(self, mock_get_time_intervals_to_sync, mock_log_sync_workflow, mock_endpoints):
        # Set up the mocks
        mock_deployment_manager_client = MagicMock()
        mock_deployment_manager_client.get_all_values_from_table.return_value = {"workflow1": "config1"}
        mock_workflow_summary_client = MagicMock()
        mock_workflow_summary_client.get_value_from_table.return_value = '{"last_sync_time": "time1"}'

        # Configure the mock_endpoints
        mock_endpoints.return_value.get_deployment_manager_client.return_value = mock_deployment_manager_client
        mock_endpoints.return_value.get_datastore_client.return_value = mock_workflow_summary_client

        # Create a LogSyncer instance
        log_syncer = LogSyncer()

        # Call the method
        log_syncer.sync()

        # Check that the mocks were called as expected
        mock_deployment_manager_client.get_all_values_from_table.assert_called_once()
        mock_workflow_summary_client.get_value_from_table.assert_called_once()
        mock_get_time_intervals_to_sync.assert_called_once()
        mock_log_sync_workflow.assert_called_once_with(
            "workflow1",
            log_syncer._region_clients,
            "config1",
            ["interval1"],
            mock_workflow_summary_client,
            {"last_sync_time": "time1"},
        )
        mock_log_sync_workflow.return_value.sync_workflow.assert_called_once()

    @patch("caribou.syncers.log_syncer.datetime")
    def test_get_time_intervals_to_sync(self, mock_datetime):
        # Create a LogSyncer instance
        log_syncer = LogSyncer()

        # Set up the test data
        current_time = datetime.now(GLOBAL_TIME_ZONE)
        mock_datetime.now.return_value = current_time
        mock_datetime.strptime = datetime.strptime

        last_sync_time = (current_time - timedelta(days=1)).strftime(TIME_FORMAT)

        # Call the method
        result = log_syncer._get_time_intervals_to_sync(last_sync_time)

        # Check that the result is as expected
        expected_result = [(datetime.strptime(last_sync_time, TIME_FORMAT), current_time)]
        self.assertEqual(result, expected_result)


if __name__ == "__main__":
    unittest.main()
