import json
import time
from typing import Any

from multi_x_serverless.data_collector.components.data_exporter import DataExporter
from multi_x_serverless.deployment.common.remote_client.remote_client import RemoteClient


class ProviderExporter(DataExporter):
    def __init__(
        self,
        client: RemoteClient,
        at_region_table: str,
        from_to_region_table: str,
        at_provider_table: str,
    ) -> None:
        super().__init__(client, at_region_table, from_to_region_table)
        self._at_provider_table = at_provider_table

    def export_all_data(self, at_region_data: dict[str, Any], from_to_region_data: dict[str, Any]) -> None:
        self._export_region_data(at_region_data, from_to_region_data)

    def export_available_region_table(self, available_region_data: dict[str, dict[str, Any]]) -> None:
        for key, value in available_region_data.items():
            data_json: str = json.dumps(value)
            current_timestamp: float = time.time()
            self._client.set_value_in_composite_key_table(
                self._available_region_table, key, current_timestamp, data_json
            )

    # def __init__(
    #     self,
    #     at_region_table: str,
    #     from_to_region_table: str,
    #     at_instance_table: str,
    #     from_to_instance_table: str,
    #     client: RemoteClient,
    # ) -> None:
    #     super().__init__(at_region_table, from_to_region_table, client)
    #     self._at_instance_table = at_instance_table
    #     self._from_to_instance_table = from_to_instance_table

    # def export_all_data(
    #     self,
    #     at_region_data: dict[str, Any],
    #     from_to_region_data: dict[str, Any],
    #     at_instance_data: dict[str, Any],
    #     from_to_instance_data: dict[str, Any],
    # ) -> bool:
    #     super().export_all_data(at_region_data, from_to_region_data)

    #     # For instance tables
    #     self._export_data(self._at_instance_table, at_instance_data)
    #     self._export_data(self._from_to_instance_table, from_to_instance_data)
