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
        # self._datacenter_data = self._retrieve_region_data(available_regions)
        self._datacenter_data = {
            "aws:region1": {
                "execution_cost": {
                    "invocation_cost": {"arm64": 2.3e-7, "x86_64": 2.3e-7, "free_tier_invocations": 1000000},
                    "compute_cost": {"arm64": 1.56138e-5, "x86_64": 1.95172e-5, "free_tier_compute_gb_s": 400000},
                    "unit": "USD",
                },
                "transmission_cost": {"global_data_transfer": 0.09, "provider_data_transfer": 0.02, "unit": "USD/GB"},
                "pue": 1.15,
                "cfe": 0.9,
                "average_memory_power": 3.92e-6,
                "average_cpu_power": 0.00212,
                "available_architectures": ["arm64", "x86_64"],
            }
        }

        # Get the set of providers from the available regions
        providers = set()
        for provider, _ in available_regions:
            providers.add(provider)
        self._provider_data = self._retrieve_provider_data(providers)  # ignore as not yet implemented

    def get_datacenter_data(self) -> dict[str, Any]:
        return self._datacenter_data

    def _retrieve_provider_data(self, available_providers: set[str]) -> dict[str, Any]:
        all_data: dict[str, Any] = {}

        for provider in available_providers:
            all_data[provider] = self._retrive_data(self._provider_table, provider)

        return all_data
