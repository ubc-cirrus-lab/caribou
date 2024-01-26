from multi_x_serverless.data_collector.components.data_exporter import DataExporter
from multi_x_serverless.deployment.common.remote_client.remote_client import RemoteClient

from typing import Any

class ProviderExporter(DataExporter):
    def __init__(self, at_region_table: str, from_to_region_table: str, client: RemoteClient) -> None:
        super().__init__(at_region_table, from_to_region_table, client)

    def export_all_data(self, at_region_data: dict[str, Any], from_to_region_data: dict[tuple[str, str], Any]) -> bool:
        return self._export_data(self._at_region_table, at_region_data, self._from_to_region_table, from_to_region_data)