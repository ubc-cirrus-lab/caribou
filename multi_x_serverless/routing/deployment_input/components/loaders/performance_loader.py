from typing import Any

from multi_x_serverless.common.constants import (
    PERFORMANCE_REGION_TABLE,
    SOLVER_INPUT_RELATIVE_PERFORMANCE_DEFAULT,
    SOLVER_INPUT_TRANSMISSION_LATENCY_DEFAULT,
)
from multi_x_serverless.common.models.remote_client.remote_client import RemoteClient
from multi_x_serverless.routing.deployment_input.components.loader import InputLoader


class PerformanceLoader(InputLoader):
    _performance_data: dict[str, Any]
    _placeholder: float  # To surpress warning, will remove later.

    def __init__(self, client: RemoteClient) -> None:
        super().__init__(client, PERFORMANCE_REGION_TABLE)

    def setup(self, available_regions: set[str]) -> None:
        self._performance_data = self._retrieve_region_data(available_regions)

    def get_relative_performance(self, region_name: str) -> float:
        return self._performance_data.get(region_name, {}).get(
            "relative_performance", SOLVER_INPUT_RELATIVE_PERFORMANCE_DEFAULT
        )

    def get_transmission_latency_distribution(
        self, from_region_name: str, to_region_name: str, data_transfer_size: float
    ) -> list[float]:
        self._placeholder = data_transfer_size
        # TODO (#121): Implement data transfer size dependent performance data
        # Currently, performance data is not dependent on data transfer size, this should be implemented later.
        return (
            self._performance_data.get(from_region_name, {})
            .get("transmission_latency", {})
            .get(to_region_name, {})
            .get("latency_distribution", [SOLVER_INPUT_TRANSMISSION_LATENCY_DEFAULT])
        )

    def get_performance_data(self) -> dict[str, Any]:
        return self._performance_data
