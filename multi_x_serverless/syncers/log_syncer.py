import json
import logging
from datetime import datetime, timedelta
from typing import Optional

from multi_x_serverless.common.constants import (
    DEPLOYMENT_MANAGER_RESOURCE_TABLE,
    FORGETTING_TIME_DAYS,
    GLOBAL_TIME_ZONE,
    TIME_FORMAT,
    WORKFLOW_SUMMARY_TABLE,
)
from multi_x_serverless.common.models.endpoints import Endpoints
from multi_x_serverless.common.models.remote_client.remote_client import RemoteClient
from multi_x_serverless.syncers.log_sync_workflow import LogSyncWorkflow

logger = logging.getLogger(__name__)


class LogSyncer:
    def __init__(self) -> None:
        self.endpoints = Endpoints()
        self._workflow_summary_client = self.endpoints.get_datastore_client()
        self._deployment_manager_client = self.endpoints.get_deployment_manager_client()
        self._region_clients: dict[tuple[str, str], RemoteClient] = {}

    def sync(self) -> None:
        currently_deployed_workflows = self._deployment_manager_client.get_all_values_from_table(
            DEPLOYMENT_MANAGER_RESOURCE_TABLE
        )

        for workflow_id, deployment_manager_config_str in currently_deployed_workflows.items():
            previous_data_str = self._workflow_summary_client.get_value_from_table(WORKFLOW_SUMMARY_TABLE, workflow_id)
            previous_data = json.loads(previous_data_str) if previous_data_str else {}

            last_sync_time: Optional[str] = previous_data.get(
                "last_sync_time",
                None,
            )

            time_intervals_to_sync = self._get_time_intervals_to_sync(last_sync_time)

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

    def _get_time_intervals_to_sync(self, last_sync_time: Optional[str]) -> list[tuple[datetime, datetime]]:
        current_time = datetime.now(GLOBAL_TIME_ZONE)
        start_time = current_time - timedelta(days=FORGETTING_TIME_DAYS)
        if last_sync_time is not None:
            last_sync_time_datetime = datetime.strptime(last_sync_time, TIME_FORMAT)
            if last_sync_time_datetime > start_time:
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
