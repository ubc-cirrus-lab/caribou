from multi_x_serverless.common.constants import AVAILABLE_REGIONS_TABLE
from multi_x_serverless.common.models.remote_client.remote_client import RemoteClient
from multi_x_serverless.routing.deployment_input.components.loader import InputLoader


class RegionViabilityLoader(InputLoader):
    def __init__(self, client: RemoteClient) -> None:
        super().__init__(client, AVAILABLE_REGIONS_TABLE)
        self._available_regions: list[str] = []

    def setup(self) -> None:
        all_regions = self._client.get_keys(self._primary_table)

        # TODO: Check if the available regions are updated
        # Now go through the available regions and only select the ones
        # With sufficiently updated timestamp on data collector columns.
        # Use region data for filtering

        # Regions are in the format: {provider}:{region_code}
        # We need to split this to get the provider and region code
        self._available_regions = all_regions

    def get_available_regions(self) -> list[str]:
        return self._available_regions
