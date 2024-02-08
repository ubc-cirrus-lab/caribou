from typing import Any

from multi_x_serverless.common.constants import PROVIDER_REGION_TABLE, PROVIDER_TABLE
from multi_x_serverless.common.models.remote_client.remote_client import RemoteClient
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
        self._provider_data = self._retrieve_provider_data(providers)  # ignore as not yet implemented

    def get_average_cpu_power(self, region_name: str) -> float:
        return self._datacenter_data.get(region_name, {}).get("average_cpu_power", 100.0)  # Default to 100 if not found

    def get_average_memory_power(self, region_name: str) -> float:
        return self._datacenter_data.get(region_name, {}).get(
            "average_memory_power", 100.0
        )  # Default to 100 if not found

    def get_pue(self, region_name: str) -> float:
        return self._datacenter_data.get(region_name, {}).get("pue", 1.0)  # Default to 1 if not found

    def get_cfe(self, region_name: str) -> float:
        return self._datacenter_data.get(region_name, {}).get("cfe", 0.0)  # Default to 0 if not found

    def get_compute_cost(self, region_name: str, architecture: str) -> float:
        return (
            self._datacenter_data.get(region_name, {})
            .get("execution_cost", {})
            .get("compute_cost", {})
            .get(architecture, 100.0)
        )  # Default to 100 if not found

    def get_invocation_cost(self, region_name: str, architecture: str) -> float:
        return (
            self._datacenter_data.get(region_name, {})
            .get("execution_cost", {})
            .get("invocation_cost", {})
            .get(architecture, 100.0)
        )  # Default to 100 if not found

    def get_transmission_cost(self, region_name: str, intra_provider_transfer: bool) -> float:
        transfer_type = "provider_data_transfer" if intra_provider_transfer else "global_data_transfer"

        return (
            self._datacenter_data.get(region_name, {}).get("transmission_cost", {}).get(transfer_type, 100.0)
        )  # Default to 100 if not found

    def _retrieve_provider_data(self, available_providers: set[str]) -> dict[str, Any]:
        all_data: dict[str, Any] = {}

        for provider in available_providers:
            all_data[provider] = self._retrive_data(self._provider_table, provider)

        return all_data
