from typing import Any

from multi_x_serverless.common.constants import PERFORMANCE_REGION_TABLE
from multi_x_serverless.deployment.common.remote_client.remote_client import RemoteClient
from multi_x_serverless.routing.solver.input.components.loader import InputLoader


class PerformanceLoader(InputLoader):
    _performance_data: dict[str, Any]

    def __init__(self, client: RemoteClient) -> None:
        super().__init__(client, PERFORMANCE_REGION_TABLE)

    def setup(self, available_regions: list[tuple[str, str]]) -> None:
        self._performance_data = self._retrieve_region_data(available_regions)

    def get_performance_data(self) -> dict[str, Any]:
        return self._performance_data
