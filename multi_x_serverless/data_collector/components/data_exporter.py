import json
import time
from abc import ABC
from typing import Any

from multi_x_serverless.common.constants import AVAILABLE_REGIONS_TABLE
from multi_x_serverless.common.models.remote_client.remote_client import RemoteClient


class DataExporter(ABC):
    def __init__(self, client: RemoteClient, region_table: str) -> None:
        self._client = client
        self._available_region_table = AVAILABLE_REGIONS_TABLE
        self._region_table = region_table
        self._modified_regions: set[str] = set()

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

    def _export_data(self, table_name: str, data: dict[str, Any], update_modified_regions: bool = False) -> None:
        """
        Exports all the processed data to all appropriate tables.

        Every region based data dictionary needs to have keys in the followin
        format:
        - <provider>:<region>   (e.g. aws:region1)

        All additional keys depend on the table being exported to.
        """
        for key, value in data.items():
            data_json: str = json.dumps(value)
            is_key_in_table: bool = self._client.get_key_present_in_table(table_name, key)
            if is_key_in_table:
                self._client.update_value_in_table(table_name, key, data_json)
            else:
                self._client.set_value_in_table(table_name, key, data_json)

            if update_modified_regions:
                provider, region = key.split(":")
                if not provider or not region:
                    raise ValueError("Data dictionary key is in invalid format.")

                self._update_modified_regions(provider, region)
