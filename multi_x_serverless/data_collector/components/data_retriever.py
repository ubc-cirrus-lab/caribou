from abc import ABC
from typing import Any
import json

from multi_x_serverless.common.constants import AVAILABLE_REGIONS_TABLE
from multi_x_serverless.common.models.remote_client.remote_client import RemoteClient


class DataRetriever(ABC):
    def __init__(self, client: RemoteClient) -> None:
        self._client = client
        self._available_region_table = AVAILABLE_REGIONS_TABLE
        self._available_regions: dict[str, dict[str, Any]] = {}

    def retrieve_available_regions(self) -> dict[str, dict[str, Any]]:
        available_regions = self._client.get_all_values_from_table(self._available_region_table)
        for region_key, available_region in available_regions.items():
            self._available_regions[region_key] = json.loads(available_region)
        return self._available_regions
