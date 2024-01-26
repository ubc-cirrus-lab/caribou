import json
from abc import ABC, abstractmethod
from typing import Any

from multi_x_serverless.deployment.common.remote_client.remote_client import RemoteClient


class DataExporter(ABC):
    _client: RemoteClient
    _at_region_table: str
    _from_to_region_table: str

    def __init__(self, at_region_table: str, from_to_region_table: str, client: RemoteClient) -> None:
        self._at_region_table = at_region_table
        self._from_to_region_table = from_to_region_table
        self._client = client

    @abstractmethod
    def export_all_data(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError

    def _export_region_data(self, at_region_data: dict[str, Any], from_to_region_data: dict[str, Any]) -> None:
        self._export_data(self._at_region_table, at_region_data)
        self._export_data(self._from_to_region_table, from_to_region_data)

    def _export_data(self, table_name: str, data: dict[str, Any]) -> None:
        """
        Exports all the processed data to all appropriate tables.
        """
        for key, value in data.items():
            data_json: str = json.dumps(value)
            self._client.set_value_in_table(table_name, key, data_json)
