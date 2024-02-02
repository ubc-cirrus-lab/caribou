from abc import ABC
from typing import Any

from multi_x_serverless.common.constants import AVAILABLE_REGIONS_TABLE
from multi_x_serverless.common.models.remote_client.remote_client import RemoteClient


class DataRetriever(ABC):
    def __init__(self, client: RemoteClient) -> None:
        self._client = client
        self._available_region_table = AVAILABLE_REGIONS_TABLE
        self._available_regions: dict[str, dict[str, Any]] = {}

    def retrieve_available_regions(self) -> dict[str, dict[str, Any]]:
        self._available_regions = self._client.get_all_values_from_table(self._available_region_table)
        return self._available_regions
