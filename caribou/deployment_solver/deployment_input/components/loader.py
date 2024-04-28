import json
from abc import ABC
from typing import Any

from caribou.common.models.remote_client.remote_client import RemoteClient


class InputLoader(ABC):
    def __init__(self, client: RemoteClient, primary_table: str) -> None:
        self._client: RemoteClient = client
        self._primary_table: str = primary_table

    def _retrieve_region_data(self, available_regions: set[str]) -> dict[str, Any]:
        all_data: dict[str, Any] = {}

        for region in available_regions:
            all_data[region] = self._retrieve_data(self._primary_table, region)

        return all_data

    def _retrieve_data(self, table_name: str, data_key: str) -> dict[str, Any]:
        value = self._client.get_value_from_table(table_name, data_key)

        loaded_data: dict[str, Any] = {}
        if value is not None and value != "":
            loaded_data = json.loads(value)

        return loaded_data

    def __str__(self) -> str:
        return f"InputLoader(name={self.__class__.__name__})"

    def __repr__(self) -> str:
        return self.__str__()
