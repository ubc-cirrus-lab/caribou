from typing import Any

from multi_x_serverless.common.constants import PERFORMANCE_REGION_TABLE
from multi_x_serverless.common.models.remote_client.remote_client import RemoteClient
from multi_x_serverless.routing.solver.input.components.loader import InputLoader


class PerformanceLoader(InputLoader):
    _performance_data: dict[str, Any]

    def __init__(self, client: RemoteClient) -> None:
        super().__init__(client, PERFORMANCE_REGION_TABLE)

    def setup(self, available_regions: set[str]) -> None:
        self._performance_data = self._retrieve_region_data(available_regions)

    def get_relative_performance(self, region_name: str) -> float:
        return self._performance_data.get(region_name, {}).get("relative_performance", 1.0)  # Default to 1 if not found

    # pylint: disable=unused-argument
    def get_transmission_latency(
        self, from_region_name: str, to_region_name: str, data_transfer_size: float, use_tail_runtime: bool = False
    ) -> float:
        latency_type = "tail_latency" if use_tail_runtime else "average_latency"

        # Currently, performance data is not dependent on data transfer size, this should be implemented leter.
        return (
            self._performance_data.get(from_region_name, {})
            .get("transmission_latency", {})
            .get(to_region_name, {})
            .get(latency_type, 100.0)
        )  # Default to huge number if not found

    def get_performance_data(self) -> dict[str, Any]:
        return self._performance_data
