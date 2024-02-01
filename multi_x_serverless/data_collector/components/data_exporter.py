import json
import time
from abc import ABC, abstractmethod
from typing import Any, Optional

from multi_x_serverless.common.constants import AVAILABLE_REGIONS_TABLE
from multi_x_serverless.deployment.common.remote_client.remote_client import RemoteClient


class DataExporter(ABC):
    def __init__(self, client: RemoteClient, region_table: str) -> None:
        self._client = client
        self._available_region_table = AVAILABLE_REGIONS_TABLE
        self._region_table = region_table
        self._modified_regions: set[str] = set()

    @abstractmethod
    def export_all_data(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError

    def update_available_region_timestamp(self, data_collector_name: str, modified_regions: set[str]) -> None:
        current_timestamp: float = time.time()
        for region in modified_regions:
            self._client.set_value_in_table_column(
                self._available_region_table,
                region,
                column_type_value=[(data_collector_name, "N", str(current_timestamp))],
            )

    def get_modified_regions(self) -> set[str]:
        return self._modified_regions

    def _export_region_data(self, region_data: dict[str, Any]) -> None:
        self._export_data(self._region_table, region_data, True)

    def _update_modified_regions(self, provider: str, region: str) -> None:
        self._modified_regions.add(f"{provider}:{region}")

    def _export_data(self, table_name: str, data: dict[str, Any], is_region_data: bool = False) -> None:
        """
        Exports all the processed data to all appropriate tables.

        Every data dictionary needs to have the following keys:
        - provider: str
        - region: str

        All additional keys depend on the table being exported to.
        """
        for key, value in data.items():
            data_json: str = json.dumps(value)
            self._client.set_value_in_table(table_name, key, data_json)

            if is_region_data:
                if "provider" not in value or "region" not in value:
                    raise ValueError("Data dictionary must have provider and region keys")
                self._update_modified_regions(value["provider"], value["region"])
