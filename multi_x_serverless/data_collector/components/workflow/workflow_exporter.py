from typing import Any

from multi_x_serverless.data_collector.components.data_exporter import DataExporter
from multi_x_serverless.deployment.common.remote_client.remote_client import RemoteClient


class WorkflowExporter(DataExporter):
    def export_all_data(self, workflow_instance_data: dict[str, Any]) -> None:
        self._export_region_data(workflow_instance_data)
