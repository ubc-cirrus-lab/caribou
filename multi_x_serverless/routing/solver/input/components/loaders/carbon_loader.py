from typing import Any

from multi_x_serverless.common.constants import (
    CARBON_REGION_TABLE,
    SOLVER_INPUT_GRID_CARBON_DEFAULT,
    SOLVER_INPUT_TRANSMISSION_CARBON_DEFAULT,
)
from multi_x_serverless.common.models.remote_client.remote_client import RemoteClient
from multi_x_serverless.routing.solver.input.components.loader import InputLoader


class CarbonLoader(InputLoader):
    _carbon_data: dict[str, Any]

    def __init__(self, client: RemoteClient) -> None:
        super().__init__(client, CARBON_REGION_TABLE)

    def setup(self, available_regions: set[str]) -> None:
        self._carbon_data = self._retrieve_region_data(available_regions)

    def get_transmission_carbon_intensity(self, from_region_name: str, to_region_name: str) -> tuple[float, float]:
        return (
            self._carbon_data.get(from_region_name, {})
            .get("transmission_carbon", {})
            .get(to_region_name, {})
            .get("carbon_intensity", SOLVER_INPUT_TRANSMISSION_CARBON_DEFAULT),
            self._carbon_data.get(from_region_name, {})
            .get("transmission_carbon", {})
            .get(to_region_name, {})
            .get("distance", SOLVER_INPUT_TRANSMISSION_CARBON_DEFAULT),
        )

    def get_grid_carbon_intensity(self, region_name: str) -> float:
        return self._carbon_data.get(region_name, {}).get("carbon_intensity", SOLVER_INPUT_GRID_CARBON_DEFAULT)
