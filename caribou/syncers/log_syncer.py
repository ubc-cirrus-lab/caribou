import json
import logging
import os
from datetime import datetime, timedelta
from typing import Optional

from caribou.common.constants import (
    BUFFER_LAMBDA_INSIGHTS_GRACE_PERIOD,
    DEPLOYMENT_RESOURCES_TABLE,
    FORGETTING_TIME_DAYS,
    GLOBAL_TIME_ZONE,
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
    def __init__(self) -> None:
        self.endpoints = Endpoints()
        self._workflow_summary_client = self.endpoints.get_datastore_client()
        self._deployment_manager_client = self.endpoints.get_deployment_resources_client()
        self._region_clients: dict[tuple[str, str], RemoteClient] = {}

    def sync(self) -> None:
        logger.info("Syncing logs for all workflows")
        currently_deployed_workflows = self._deployment_manager_client.get_all_values_from_table(
            DEPLOYMENT_RESOURCES_TABLE
        )

        for workflow_id, deployment_manager_config_str in currently_deployed_workflows.items():
            logger.info("Syncing logs for workflow %s", workflow_id)
            previous_data_str, _ = self._workflow_summary_client.get_value_from_table(
                WORKFLOW_SUMMARY_TABLE, workflow_id
            )
            previous_data = json.loads(previous_data_str) if previous_data_str else {}

            last_sync_time: Optional[str] = previous_data.get(
                "last_sync_time",
                None,
            )

            time_intervals_to_sync = self._get_time_intervals_to_sync(
                last_sync_time, BUFFER_LAMBDA_INSIGHTS_GRACE_PERIOD
            )

            if len(time_intervals_to_sync) == 0:
                continue

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
        self, last_sync_time: Optional[str], buffer_minutes: float = 5
    ) -> list[tuple[datetime, datetime]]:
        current_time = datetime.now(GLOBAL_TIME_ZONE)

        # Subtract buffer time to avoid missing logs
        # As AWS insights might take some time to be available
        current_time -= timedelta(minutes=buffer_minutes)

        start_time = current_time - timedelta(days=FORGETTING_TIME_DAYS)
        if last_sync_time is not None:
            last_sync_time_datetime = datetime.strptime(last_sync_time, TIME_FORMAT)
            if last_sync_time_datetime > start_time:  # pylint: disable=consider-using-max-builtin
                start_time = last_sync_time_datetime

        time_intervals_to_sync = []
        while start_time < current_time:
            end_time = start_time + timedelta(days=1)
            if end_time > current_time:  # pylint: disable=consider-using-min-builtin
                end_time = current_time
            time_intervals_to_sync.append((start_time, end_time))
            start_time = end_time
            if start_time >= current_time:
                break

        return time_intervals_to_sync
