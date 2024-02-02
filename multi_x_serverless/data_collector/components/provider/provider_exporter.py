from typing import Any

from multi_x_serverless.common.models.remote_client.remote_client import RemoteClient
from multi_x_serverless.data_collector.components.data_exporter import DataExporter


class ProviderExporter(DataExporter):
    def __init__(
        self,
        client: RemoteClient,
        provider_region_table: str,
        provider_table: str,
    ) -> None:
        super().__init__(client, provider_region_table)
        self.provider_table = provider_table

    def export_all_data(self, provider_region_data: dict[str, Any], provider_data: dict[str, Any]) -> None:
        self._export_region_data(provider_region_data)
        self._export_data(self.provider_table, provider_data, True)

    def export_available_region_table(self, available_region_data: dict[str, dict[str, Any]]) -> None:
        self._export_data(self._available_region_table, available_region_data, True)
