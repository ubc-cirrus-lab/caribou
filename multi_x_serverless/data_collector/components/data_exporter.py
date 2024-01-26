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
    def export_all_data(self, *args: Any, **kwargs: Any) -> bool:
        raise NotImplementedError

    def _export_data(
        self,
        at_table_name: str,
        at_data: dict[str, Any],
        from_to_table_name: str,
        from_to_data: dict[tuple[str, str], Any],
    ) -> bool:
        """
        Exports all the processed data to all appropriate tables.
        """
        success = True
        success &= self._export_at_data(at_table_name, at_data)
        success &= self._export_from_to_data(from_to_table_name, from_to_data)
        return success

    def _export_at_data(self, table_name: str, at_data: dict[str, Any]) -> bool:
        """
        Exports the "at" resource data from processed data.
        """
        for key, value in at_data.items():
            data_json = json.dumps(value)
            # self._client.set_value_in_table(table_name, key, data_json) # Not sufficient 

        # Also: Should we alter client to have bool to denote success?
        return False  # Not yet implemented 

    def _export_from_to_data(self, table_name: str, from_to_data: dict[tuple[str, str], Any]) -> bool:
        """
        Exports the "from_to" resource data from processed data.
        """
        for key, value in from_to_data.items():
            primary_key, secondary_key = key
            data_json = json.dumps(value)
            # self._client.set_value_in_table(table_name, key, data_json) # We should implement this but need to be able to have multiple keys in a table

        return False  # Not yet implemented
