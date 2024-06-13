import json
from typing import Any
import re

from caribou.common.models.remote_client.remote_client import RemoteClient
from caribou.data_collector.components.data_exporter import DataExporter

class SingleLineListEncoder(json.JSONEncoder):
    def encode(self, obj):
        if isinstance(obj, list):
            return '[' + ', '.join(self.encode(el) for el in obj) + ']'
        return super().encode(obj)


class WorkflowExporter(DataExporter):
    def __init__(self, client: RemoteClient, workflow_instance_table: str) -> None:
        super().__init__(client, "")
        self._workflow_summary_table: str = workflow_instance_table

    def export_all_data(self, workflow_summary_data: dict[str, Any]) -> None:
        self._export_workflow_summary(workflow_summary_data)

    def _export_workflow_summary(self, workflow_summary_data: dict[str, Any]) -> None:
        self._export_data(self._workflow_summary_table, workflow_summary_data, False)
