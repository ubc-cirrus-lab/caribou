import json
from typing import Any

from multi_x_serverless.data_collector.components.data_exporter import DataExporter
from multi_x_serverless.deployment.common.remote_client.remote_client import RemoteClient


class WorkflowExporter(DataExporter):
    def __init__(self, client: RemoteClient, workflow_instance_table: str) -> None:
        super().__init__(client, "")
        self._workflow_summary_table: str = workflow_instance_table

    def export_all_data(self, workflow_summary_data: dict[str, Any]) -> None:
        self._export_workflow_summary(workflow_summary_data)

    def _export_workflow_summary(self, workflow_summary_data: dict[str, Any]) -> None:
        self._export_data(self._workflow_summary_table, workflow_summary_data, False)
