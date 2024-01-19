import json
from abc import ABC, abstractmethod
from typing import Any

from multi_x_serverless.common.constants import DATA_COLLECTOR_DATA_TABLE
from multi_x_serverless.common.models.endpoints import Endpoints


class DataCollector(ABC):
    def __init__(self) -> None:
        self._data_table = DATA_COLLECTOR_DATA_TABLE
        if not hasattr(self, "_table_key"):
            self._table_key = "ABSTRACT_TABLE_KEY"
        self._data_collector_client = Endpoints().get_data_collector_client()

    def run(self) -> None:
        data = self.collect_data()
        self.upload_data_to_table(data)

    @abstractmethod
    def collect_data(self) -> dict[str, Any]:
        """
        Collects data from the data source

        Returns:
            Dict[str, Any]: data collected from the data source
        """
        pass

    def upload_data_to_table(self, data: dict[str, Any]) -> None:
        data_json = json.dumps(data)
        self._data_collector_client.set_value_in_table(self._data_table, self._table_key, data_json)
