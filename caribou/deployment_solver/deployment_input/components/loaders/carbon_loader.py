from typing import Any, Optional

from caribou.common.constants import (  # SOLVER_INPUT_TRANSMISSION_CARBON_DEFAULT,
    CARBON_REGION_TABLE,
    SOLVER_INPUT_GRID_CARBON_DEFAULT,
)
from caribou.common.models.remote_client.remote_client import RemoteClient
from caribou.deployment_solver.deployment_input.components.loader import InputLoader


class CarbonLoader(InputLoader):
    _carbon_data: dict[str, Any]

    def __init__(
        self,
        client: RemoteClient,
    ) -> None:
        super().__init__(client, CARBON_REGION_TABLE)

    def setup(self, available_regions: set[str], carbon_data: Optional[dict[str, Any]] = None) -> None:
        if carbon_data is not None:
            self._carbon_data = carbon_data
        else:
            self._carbon_data = self._retrieve_region_data(available_regions)

    def get_transmission_distance(self, from_region_name: str, to_region_name: str) -> float:
        # TODO: Deal with the default distance
        return (
            self._carbon_data.get(from_region_name, {}).get("transmission_distances", {}).get(to_region_name, -1)
        )  # Indicate no data

    def get_grid_carbon_intensity(self, region_name: str, hour: Optional[str] = None) -> float:
        carbon_policy = hour if hour is not None else "overall"

        return (
            self._carbon_data.get(region_name, {})
            .get("averages", {})
            .get(carbon_policy, {})
            .get("carbon_intensity", SOLVER_INPUT_GRID_CARBON_DEFAULT)
        )

    def get_carbon_data(self) -> dict[str, Any]:
        return self._carbon_data

    def to_dict(self) -> dict[str, Any]:
        return self._carbon_data
