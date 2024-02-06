from typing import Any

from multi_x_serverless.common.constants import CARBON_REGION_TABLE
from multi_x_serverless.common.models.remote_client.remote_client import RemoteClient
from multi_x_serverless.routing.solver.input.components.loader import InputLoader


class CarbonLoader(InputLoader):
    _carbon_data: dict[str, Any]

    def __init__(self, client: RemoteClient) -> None:
        super().__init__(client, CARBON_REGION_TABLE)

    def setup(self, available_regions: list[tuple[str, str]]) -> None:
        # self._carbon_data = self._retrieve_region_data(available_regions)

        self._carbon_data = {
            "aws:eu-south-1": {
                "carbon_intensity": 482,
                "unit": "gCO2eq/kWh",
                "transmission_carbon": {
                    "aws:eu-south-1": {"carbon_intensity": 48.2, "unit": "gCO2eq/GB"},
                    "aws:eu-central-1": {"carbon_intensity": 1337.9261964617801, "unit": "gCO2eq/GB"},
                    "aws:us-west-2": {"carbon_intensity": 21269.19652594863, "unit": "gCO2eq/GB"},
                },
            }
        }

    def get_carbon_data(self) -> dict[str, Any]:
        return self._carbon_data
