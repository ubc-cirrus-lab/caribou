import json
import logging
import os
from datetime import datetime, timedelta
from typing import Optional

from caribou.common.constants import (
    BUFFER_LAMBDA_INSIGHTS_GRACE_PERIOD,
    DEPLOYMENT_MANAGER_RESOURCE_TABLE,
    DEPLOYMENT_RESOURCES_TABLE,
    FORGETTING_TIME_DAYS,
    GLOBAL_TIME_ZONE,
    MIN_TIME_BETWEEN_SYNC,
    TIME_FORMAT,
    WORKFLOW_SUMMARY_TABLE,
)
from caribou.common.models.endpoints import Endpoints
from caribou.common.models.remote_client.remote_client import RemoteClient
from caribou.syncers.log_sync_workflow import LogSyncWorkflow

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Only add a StreamHandler if not running in AWS Lambda
if "AWS_LAMBDA_FUNCTION_NAME" not in os.environ:
    if not logger.handlers:
        logger.addHandler(logging.StreamHandler())


class LogSyncer:
    def __init__(self, deployed_remotely: bool = False) -> None:
        self._endpoints = Endpoints()
        self._workflow_summary_client = self._endpoints.get_datastore_client()
        self._deployment_manager_client = self._endpoints.get_deployment_resources_client()
        self._region_clients: dict[tuple[str, str], RemoteClient] = {}

        # Indicates if the deployment algorithm is deployed remotely
        self._deployed_remotely: bool = deployed_remotely

    def sync(self) -> None:
        logger.info("Running Log Syncer: Sync Workflow Logs")
        workflow_ids = self._deployment_manager_client.get_keys(DEPLOYMENT_MANAGER_RESOURCE_TABLE)

        for workflow_id in workflow_ids:
            if self._deployed_remotely:
                # Initiate the deployment manager on a remote lambda function (AWS Lambda)
                self.remote_sync_workflow(workflow_id)
            else:
                # Invoke locally/same lambda function
                self.sync_workflow(workflow_id)

    def remote_sync_workflow(self, workflow_id: str) -> None:
        logger.info("Remote Syncing logs for workflow %s", workflow_id)
        framework_cli_remote_client = self._endpoints.get_framework_cli_remote_client()

        framework_cli_remote_client.invoke_remote_framework_internal_action(
            "sync_workflow",
            {"workflow_id": workflow_id},
        )

    def sync_workflow(self, workflow_id: str) -> None:
        logger.info("Checking if need syncing logs for workflow %s", workflow_id)

        deployment_manager_config_str, _ = self._deployment_manager_client.get_value_from_table(
            DEPLOYMENT_RESOURCES_TABLE, workflow_id
        )

        previous_data_str, _ = self._workflow_summary_client.get_value_from_table(WORKFLOW_SUMMARY_TABLE, workflow_id)
        previous_data = json.loads(previous_data_str) if previous_data_str else {}

        last_sync_time: Optional[str] = previous_data.get(
            "last_sync_time",
            None,
        )

        time_intervals_to_sync = self._get_time_intervals_to_sync(last_sync_time)

        if len(time_intervals_to_sync) == 0:
            return

        logger.info("Enough time has passed, syncing logs.\n")
        log_sync_workflow = LogSyncWorkflow(
            workflow_id,
            self._region_clients,
            deployment_manager_config_str,
            time_intervals_to_sync,
            self._workflow_summary_client,
            previous_data,
        )
        log_sync_workflow.sync_workflow()

    def _get_time_intervals_to_sync(
        self,
        last_sync_time: Optional[str],
        buffer_minutes: float = BUFFER_LAMBDA_INSIGHTS_GRACE_PERIOD,
        forget_days: int = FORGETTING_TIME_DAYS,
        min_minutes_between_sync: int = MIN_TIME_BETWEEN_SYNC,
    ) -> list[tuple[datetime, datetime]]:
        current_time = datetime.now(GLOBAL_TIME_ZONE)
        buffered_time = current_time - timedelta(minutes=buffer_minutes)
        start_time = current_time - timedelta(days=forget_days)

        if last_sync_time is not None:
            last_sync_time_datetime = datetime.strptime(last_sync_time, TIME_FORMAT)

            # Verify that the last sync time is at least some interval before the buffered time
            # This is to reduce wasted invocations
            if buffered_time - last_sync_time_datetime < timedelta(minutes=min_minutes_between_sync):
                return []

            start_time = max(start_time, last_sync_time_datetime)

        time_intervals_to_sync = []
        while start_time < buffered_time:
            end_time = min(start_time + timedelta(days=1), buffered_time)
            time_intervals_to_sync.append((start_time, end_time))
            start_time = end_time

        return time_intervals_to_sync
