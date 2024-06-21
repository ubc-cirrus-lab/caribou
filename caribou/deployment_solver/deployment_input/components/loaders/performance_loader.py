from typing import Any

from caribou.common.constants import (
    PERFORMANCE_REGION_TABLE,
    SOLVER_INPUT_RELATIVE_PERFORMANCE_DEFAULT,
    SOLVER_HOME_REGION_TRANSMISSION_LATENCY_DEFAULT,
)
from caribou.common.models.remote_client.remote_client import RemoteClient
from caribou.deployment_solver.deployment_input.components.loader import InputLoader


class PerformanceLoader(InputLoader):
    _performance_data: dict[str, Any]

    def __init__(self, client: RemoteClient) -> None:
        super().__init__(client, PERFORMANCE_REGION_TABLE)

    def setup(self, available_regions: set[str]) -> None:
        self._performance_data = self._retrieve_region_data(available_regions)

    def get_relative_performance(self, region_name: str) -> float:
        return self._performance_data.get(region_name, {}).get(
            "relative_performance", SOLVER_INPUT_RELATIVE_PERFORMANCE_DEFAULT
        )

    def get_transmission_latency_distribution(self, from_region_name: str, to_region_name: str) -> list[float]:
        return (
            self._performance_data.get(from_region_name, {})
            .get("transmission_latency", {})
            .get(to_region_name, {})
            .get("latency_distribution", [SOLVER_HOME_REGION_TRANSMISSION_LATENCY_DEFAULT])
        )

    def get_performance_data(self) -> dict[str, Any]:
        return self._performance_data
