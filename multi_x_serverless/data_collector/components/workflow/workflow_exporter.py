from typing import Any

from multi_x_serverless.data_collector.components.data_exporter import DataExporter
from multi_x_serverless.deployment.common.remote_client.remote_client import RemoteClient


class WorkflowExporter(DataExporter):
    _at_instance_table: str
    _from_to_instance_table: str

    def __init__(
        self,
        client: RemoteClient,
        available_region_table: str,
        at_region_table: str,
        from_to_region_table: str,
        at_instance_table: str,
        from_to_instance_table: str,
    ) -> None:
        super().__init__(client, available_region_table, at_region_table, from_to_region_table)
        self._at_instance_table = at_instance_table
        self._from_to_instance_table = from_to_instance_table

    def export_all_data(
        self,
        at_region_data: dict[str, Any],
        from_to_region_data: dict[str, Any],
        at_instance_data: dict[str, Any],
        from_to_instance_data: dict[str, Any],
    ) -> None:
        self._export_region_data(at_region_data, from_to_region_data)

        # Export instance table data
        self._export_data(self._at_instance_table, at_instance_data)
        self._export_data(self._from_to_instance_table, from_to_instance_data)
