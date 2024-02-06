from typing import Any

from multi_x_serverless.common.constants import AVAILABLE_REGIONS_TABLE
from multi_x_serverless.common.models.remote_client.remote_client import RemoteClient
from multi_x_serverless.routing.solver.input.components.loader import InputLoader


class RegionViabilityLoader(InputLoader):
    _available_regions: list[tuple[str, str]]

    def __init__(self, client: RemoteClient) -> None:
        super().__init__(client, AVAILABLE_REGIONS_TABLE)

    def setup(self) -> None:
        # all_regions = self._client.get_all_values_from_table(self._primary_table)
        all_regions = {
            "aws:eu-south-1": {
                "key": "aws:eu-south-1",
                "provider_collector": 1620000000,
                "carbon_collector": 1620000000,
                "performance_collector": 1620000000,
                "value": {
                    "name": "Europe (Milan)",
                    "provider": "aws",
                    "code": "eu-south-1",
                    "latitude": 45.4642035,
                    "longitude": 9.189982,
                },
            }
        }

        for region, region_data in all_regions.items():
            # TODO: Check if the available regions are updated
            # Now go through the available regions and only select the ones
            # With sufficiently updated timestamp on data collector columns.
            # Use region data for filtering

            # Regions is int he format of: {provider}:{region_code}
            # We need to split this to get the provider and region code
            provider, region_code = region.split(":")
            self._available_regions.append((provider, region_code))

    def get_available_regions(self) -> list[tuple[str, str]]:
        return self._available_regions
