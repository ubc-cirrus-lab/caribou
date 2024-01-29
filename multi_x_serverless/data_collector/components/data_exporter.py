import json
from abc import ABC, abstractmethod
from typing import Any
import time

from multi_x_serverless.deployment.common.remote_client.remote_client import RemoteClient


class DataExporter(ABC):
    _client: RemoteClient
    _available_region_table: str
    _at_region_table: str
    _from_to_region_table: str

    def __init__(
        self, client: RemoteClient, available_region_table: str, at_region_table: str, from_to_region_table: str
    ) -> None:
        self._client = client
        self._available_region_table = available_region_table
        self._at_region_table = at_region_table
        self._from_to_region_table = from_to_region_table

    @abstractmethod
    def export_all_data(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError

    def update_available_region_timestamp(self, data_collector_name: str, modified_regions: list[str]) -> None:
        for region in modified_regions:
            current_timestamp: float = time.time()
            self._client.update_timestamp_in_composite_key_table(
                self._available_region_table, region, current_timestamp
            )

    def _export_region_data(
        self, at_region_data: dict[str, dict[str, Any]], from_to_region_data: dict[str, Any]
    ) -> None:
        self._export_data(self._at_region_table, at_region_data)
        self._export_data(self._from_to_region_table, from_to_region_data)

    def _export_data(self, table_name: str, data: dict[str, dict[str, Any]]) -> None:
        """
        Exports all the processed data to all appropriate tables.
        """
        for key, value in data.items():
            data_json: str = json.dumps(value)
            self._client.set_value_in_table(table_name, key, data_json)
