import json
from abc import ABC, abstractmethod
from typing import Any

from multi_x_serverless.deployment.common.remote_client.remote_client import RemoteClient


class InputLoader(ABC):
    def __init__(self, client: RemoteClient, primary_table: str) -> None:
        self._client: RemoteClient = client
        self._primary_table: str = primary_table

    @abstractmethod
    def setup(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError

    def _retrieve_region_data(self, available_regions: list[tuple[str, str]]) -> dict[str, Any]:
        all_data: dict[str, Any] = {}

        for region_info in available_regions:
            data_key = f"{region_info[0]}:{region_info[1]}"
            all_data[data_key] = self._retrive_data(self._primary_table, data_key)

        return all_data

    def _retrive_data(self, table_name: str, data_key: str) -> dict[str, Any]:
        loaded_data: dict[str, Any] = json.loads(self._client.get_value_from_table(table_name, data_key))
        return loaded_data

    def __str__(self) -> str:
        return f"InputLoader(name={self.__class__.__name__})"

    def __repr__(self) -> str:
        return self.__str__()
