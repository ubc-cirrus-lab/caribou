import json
import time
from abc import ABC, abstractmethod
from typing import Any

from multi_x_serverless.common.constants import AVAILABLE_REGIONS_TABLE
from multi_x_serverless.deployment.common.remote_client.remote_client import RemoteClient


class DataExporter(ABC):
    def __init__(self, client: RemoteClient, at_region_table: str, from_to_region_table: str) -> None:
        self._client = client
        self._available_region_table = AVAILABLE_REGIONS_TABLE
        self._at_region_table = at_region_table
        self._from_to_region_table = from_to_region_table
        self._modified_regions: set[str] = set()

    @abstractmethod
    def export_all_data(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError

    def update_available_region_timestamp(self, data_collector_name: str, modified_regions: set[str]) -> None:
        for region in modified_regions:
            current_timestamp: float = time.time()
            self._client.update_timestamp_in_composite_key_table(
                self._available_region_table, region, current_timestamp
            )

    def get_modified_regions(self) -> set[str]:
        return self._modified_regions

    def _export_region_data(
        self, at_region_data: dict[str, dict[str, Any]], from_to_region_data: dict[str, Any]
    ) -> None:
        self._export_data(self._at_region_table, at_region_data)
        self._export_data(self._from_to_region_table, from_to_region_data)

    def _update_modified_regions(self, provider: str, region: str) -> None:
        self._modified_regions.add(f"{provider}_{region}")

    def _export_data(self, table_name: str, data: dict[str, dict[str, Any]]) -> None:
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

            if "provider" not in value or "region" not in value:
                raise ValueError("Data dictionary must have provider and region keys")
            self._update_modified_regions(value["provider"], value["region"])