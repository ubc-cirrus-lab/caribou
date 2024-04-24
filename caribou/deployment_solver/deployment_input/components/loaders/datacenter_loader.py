from typing import Any

from caribou.common.constants import (
    PROVIDER_REGION_TABLE,
    PROVIDER_TABLE,
    SOLVER_INPUT_AVERAGE_CPU_POWER_DEFAULT,
    SOLVER_INPUT_AVERAGE_MEMORY_POWER_DEFAULT,
    SOLVER_INPUT_CFE_DEFAULT,
    SOLVER_INPUT_COMPUTE_COST_DEFAULT,
    SOLVER_INPUT_INVOCATION_COST_DEFAULT,
    SOLVER_INPUT_PUE_DEFAULT,
    SOLVER_INPUT_TRANSMISSION_COST_DEFAULT,
)
from caribou.common.models.remote_client.remote_client import RemoteClient
from caribou.deployment_solver.deployment_input.components.loader import InputLoader


class DatacenterLoader(InputLoader):
    _datacenter_data: dict[str, Any]
    _provider_data: dict[str, Any]
    _provider_table: str

    def __init__(self, client: RemoteClient) -> None:
        super().__init__(client, PROVIDER_REGION_TABLE)
        self._provider_table = PROVIDER_TABLE

    def setup(self, available_regions: set[str]) -> None:
        self._datacenter_data = self._retrieve_region_data(available_regions)

        # Get the set of providers from the available regions
        providers = set()
        for region in available_regions:
            provider, _ = region.split(":")
            providers.add(provider)

        self._provider_data = self._retrieve_provider_data(providers)  # ignore as not yet implemented

    def get_average_cpu_power(self, region_name: str) -> float:
        return self._datacenter_data.get(region_name, {}).get(
            "average_cpu_power", SOLVER_INPUT_AVERAGE_CPU_POWER_DEFAULT
        )

    def get_average_memory_power(self, region_name: str) -> float:
        return self._datacenter_data.get(region_name, {}).get(
            "average_memory_power", SOLVER_INPUT_AVERAGE_MEMORY_POWER_DEFAULT
        )

    def get_pue(self, region_name: str) -> float:
        return self._datacenter_data.get(region_name, {}).get("pue", SOLVER_INPUT_PUE_DEFAULT)

    def get_cfe(self, region_name: str) -> float:
        return self._datacenter_data.get(region_name, {}).get("cfe", SOLVER_INPUT_CFE_DEFAULT)

    def get_compute_cost(self, region_name: str, architecture: str) -> float:
        return (
            self._datacenter_data.get(region_name, {})
            .get("execution_cost", {})
            .get("compute_cost", {})
            .get(architecture, SOLVER_INPUT_COMPUTE_COST_DEFAULT)
        )

    def get_invocation_cost(self, region_name: str, architecture: str) -> float:
        return (
            self._datacenter_data.get(region_name, {})
            .get("execution_cost", {})
            .get("invocation_cost", {})
            .get(architecture, SOLVER_INPUT_INVOCATION_COST_DEFAULT)
        )

    def get_transmission_cost(self, region_name: str, intra_provider_transfer: bool) -> float:
        transfer_type = "provider_data_transfer" if intra_provider_transfer else "global_data_transfer"

        return (
            self._datacenter_data.get(region_name, {})
            .get("transmission_cost", {})
            .get(transfer_type, SOLVER_INPUT_TRANSMISSION_COST_DEFAULT)
        )

    def _retrieve_provider_data(self, available_providers: set[str]) -> dict[str, Any]:
        all_data: dict[str, Any] = {}

        for provider in available_providers:
            all_data[provider] = self._retrieve_data(self._provider_table, provider)

        return all_data
