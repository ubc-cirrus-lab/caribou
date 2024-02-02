from typing import Any

from multi_x_serverless.common.constants import PROVIDER_REGION_TABLE, PROVIDER_TABLE
from multi_x_serverless.deployment.common.remote_client.remote_client import RemoteClient
from multi_x_serverless.routing.solver.input.components.loader import InputLoader


class DatacenterLoader(InputLoader):
    _datacenter_data: dict[str, Any]
    _provider_data: dict[str, Any]
    _provider_table: str

    def __init__(self, client: RemoteClient) -> None:
        super().__init__(client, PROVIDER_REGION_TABLE)
        self._provider_table = PROVIDER_TABLE

    def setup(self, available_regions: list[tuple[str, str]]) -> None:
        self._datacenter_data = self._retrieve_region_data(available_regions)

        # Get the set of providers from the available regions
        providers = set()
        for provider, _ in available_regions:
            providers.add(provider)
        self._provider_data = self._retrieve_provider_data(providers)

    def get_datacenter_data(self) -> dict[str, Any]:
        return self._datacenter_data

    def _retrieve_provider_data(self, available_providers: set[str]) -> dict[str, Any]:
        all_data: dict[str, Any] = {}

        for provider in available_providers:
            all_data[provider] = self._retrive_data(self._provider_table, provider)

        return all_data
