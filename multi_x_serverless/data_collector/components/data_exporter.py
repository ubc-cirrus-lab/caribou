import json
from abc import ABC, abstractmethod
from typing import Any

from multi_x_serverless.deployment.common.remote_client.remote_client import RemoteClient

class DataExporter(ABC):
    _client: RemoteClient
    _at_data_table: str
    _from_to_data_table: str

    def __init__(self, at_data_table: str, from_to_data_table: str, client: RemoteClient) -> None:
        self._at_data_table = at_data_table
        self._from_to_data_table = from_to_data_table
        self._client = client

    @abstractmethod
    def export_data(self) -> bool:
        """
        Exports the processed data from the data source
        """
        # data_json = json.dumps(data)
        # self._data_collector_client.set_value_in_table(self._data_table, self._table_key, data_json)

        return False