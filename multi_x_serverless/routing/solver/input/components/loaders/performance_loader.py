from typing import Any

from multi_x_serverless.common.constants import PERFORMANCE_REGION_TABLE
from multi_x_serverless.common.models.remote_client.remote_client import RemoteClient
from multi_x_serverless.routing.solver.input.components.loader import InputLoader


class PerformanceLoader(InputLoader):
    _performance_data: dict[str, Any]

    def __init__(self, client: RemoteClient) -> None:
        super().__init__(client, PERFORMANCE_REGION_TABLE)

    def setup(self, available_regions: list[tuple[str, str]]) -> None:
        self._performance_data = self._retrieve_region_data(available_regions)

    def get_relative_performance(self, region_name: str) -> float:
        return self._performance_data.get(region_name, {}).get("relative_performance", 1) # Default to 1 if not found

    def get_transmission_latency(self, from_region_name: str, to_region_name: str, data_transfer_size: float) -> float:
        # Currently, performance data is not dependent on data transfer size, this should be implemented leter.
        return self._performance_data.get(from_region_name, {}).get("transmission_latency", {}).get(to_region_name, {}).get("transmission_latency", 100) # Default to huge number if not found

    def get_performance_data(self) -> dict[str, Any]:
        return self._performance_data
