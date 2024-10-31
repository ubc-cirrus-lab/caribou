import json
import unittest
from unittest.mock import patch
from datetime import datetime, timedelta
from caribou.syncers.log_syncer import LogSyncer

from caribou.common.constants import (
    FORGETTING_TIME_DAYS,
    GLOBAL_TIME_ZONE,
    MIN_TIME_BETWEEN_SYNC,
    TIME_FORMAT,
)


class TestLogSyncer(unittest.TestCase):
    @patch("caribou.syncers.log_syncer.Endpoints")
    def setUp(self, MockEndpoints):
        self.mock_endpoints = MockEndpoints.return_value
        self.mock_datastore_client = self.mock_endpoints.get_datastore_client.return_value
        self.mock_deployment_resources_client = self.mock_endpoints.get_deployment_resources_client.return_value
        self.mock_framework_cli_remote_client = self.mock_endpoints.get_framework_cli_remote_client.return_value
        self.log_syncer = LogSyncer()

    @patch("caribou.syncers.log_syncer.LogSyncer.remote_sync_workflow")
    @patch("caribou.syncers.log_syncer.LogSyncer.sync_workflow")
    def test_sync(self, mock_sync_workflow, mock_remote_sync_workflow):
        self.mock_deployment_resources_client.get_keys.return_value = ["workflow_id_1", "workflow_id_2"]
        self.log_syncer.sync()

        self.assertEqual(mock_sync_workflow.call_count, 2)
        self.assertEqual(mock_remote_sync_workflow.call_count, 0)

    @patch("caribou.syncers.log_syncer.LogSyncer.remote_sync_workflow")
    @patch("caribou.syncers.log_syncer.LogSyncer.sync_workflow")
    def test_sync_remote(self, mock_sync_workflow, mock_remote_sync_workflow):
        self.log_syncer._deployed_remotely = True
        self.mock_deployment_resources_client.get_keys.return_value = ["workflow_id_1", "workflow_id_2"]
        self.log_syncer.sync()

        self.assertEqual(mock_sync_workflow.call_count, 0)
        self.assertEqual(mock_remote_sync_workflow.call_count, 2)

    @patch("caribou.syncers.log_syncer.logger")
    def test_remote_sync_workflow(self, mock_logger):
        workflow_id = "test_workflow_id"
        self.log_syncer.remote_sync_workflow(workflow_id)

        self.mock_framework_cli_remote_client.invoke_remote_framework_internal_action.assert_called_once_with(
            "sync_workflow",
            {"workflow_id": workflow_id},
        )
        mock_logger.info.assert_called_with("Remote Syncing logs for workflow %s", workflow_id)

    @patch("caribou.syncers.log_syncer.LogSyncWorkflow")
    @patch("caribou.syncers.log_syncer.logger")
    def test_sync_workflow_no_sync_needed(self, mock_logger, MockLogSyncWorkflow):
        workflow_id = "test_workflow_id"
        previous_data = {
            "last_sync_time": (datetime.now(GLOBAL_TIME_ZONE) - timedelta(minutes=MIN_TIME_BETWEEN_SYNC - 1)).strftime(
                TIME_FORMAT
            )
        }
        self.mock_deployment_resources_client.get_value_from_table.return_value = ("config_str", None)
        self.mock_datastore_client.get_value_from_table.return_value = (json.dumps(previous_data), None)

        self.log_syncer.sync_workflow(workflow_id)

        mock_logger.info.assert_called_with("Checking if need syncing logs for workflow %s", workflow_id)
        MockLogSyncWorkflow.assert_not_called()

    @patch("caribou.syncers.log_syncer.LogSyncWorkflow")
    @patch("caribou.syncers.log_syncer.logger")
    def test_sync_workflow_sync_needed(self, mock_logger, MockLogSyncWorkflow):
        workflow_id = "test_workflow_id"
        previous_data = {"last_sync_time": (datetime.now(GLOBAL_TIME_ZONE) - timedelta(days=2)).strftime(TIME_FORMAT)}
        self.mock_deployment_resources_client.get_value_from_table.return_value = ("config_str", None)
        self.mock_datastore_client.get_value_from_table.return_value = (json.dumps(previous_data), None)

        self.log_syncer.sync_workflow(workflow_id)

        mock_logger.info.assert_any_call("Checking if need syncing logs for workflow %s", workflow_id)
        mock_logger.info.assert_any_call("Enough time has passed, syncing logs.\n")
        MockLogSyncWorkflow.assert_called_once()
        MockLogSyncWorkflow.return_value.sync_workflow.assert_called_once()

    @patch("caribou.syncers.log_syncer.LogSyncWorkflow")
    @patch("caribou.syncers.log_syncer.logger")
    def test_sync_workflow_no_previous_data(self, mock_logger, MockLogSyncWorkflow):
        workflow_id = "test_workflow_id"
        self.mock_deployment_resources_client.get_value_from_table.return_value = ("config_str", None)
        self.mock_datastore_client.get_value_from_table.return_value = (None, None)

        self.log_syncer.sync_workflow(workflow_id)

        mock_logger.info.assert_any_call("Checking if need syncing logs for workflow %s", workflow_id)
        mock_logger.info.assert_any_call("Enough time has passed, syncing logs.\n")
        MockLogSyncWorkflow.assert_called_once()
        MockLogSyncWorkflow.return_value.sync_workflow.assert_called_once()

    def test_get_time_intervals_to_sync(self):
        last_sync_time = (datetime.now(GLOBAL_TIME_ZONE) - timedelta(days=2)).strftime(TIME_FORMAT)
        time_intervals = self.log_syncer._get_time_intervals_to_sync(last_sync_time)

        self.assertTrue(len(time_intervals) > 0)

    def test_get_time_intervals_to_sync_no_last_sync_time(self):
        time_intervals = self.log_syncer._get_time_intervals_to_sync(None)

        self.assertTrue(len(time_intervals) > 0)

    def test_get_time_intervals_to_sync_recent_sync(self):
        last_sync_time = (datetime.now(GLOBAL_TIME_ZONE) - timedelta(minutes=MIN_TIME_BETWEEN_SYNC - 1)).strftime(
            TIME_FORMAT
        )
        time_intervals = self.log_syncer._get_time_intervals_to_sync(last_sync_time)

        self.assertEqual(len(time_intervals), 0)

    def test_get_time_intervals_to_sync_forget_days(self):
        last_sync_time = (datetime.now(GLOBAL_TIME_ZONE) - timedelta(days=FORGETTING_TIME_DAYS + 1)).strftime(
            TIME_FORMAT
        )
        time_intervals = self.log_syncer._get_time_intervals_to_sync(last_sync_time)

        self.assertTrue(len(time_intervals) > 0)


if __name__ == "__main__":
    unittest.main()
