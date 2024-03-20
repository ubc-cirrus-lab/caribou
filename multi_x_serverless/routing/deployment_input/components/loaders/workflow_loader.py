from typing import Any

from multi_x_serverless.common.constants import (
    SOLVER_INPUT_ARCHITECTURE_DEFAULT,
    SOLVER_INPUT_INVOCATION_PROBABILITY_DEFAULT,
    SOLVER_INPUT_TRANSMISSION_LATENCY_DEFAULT,
    SOLVER_INPUT_VCPU_DEFAULT,
    WORKFLOW_INSTANCE_TABLE,
)
from multi_x_serverless.common.models.remote_client.remote_client import RemoteClient
from multi_x_serverless.common.provider import Provider
from multi_x_serverless.routing.deployment_input.components.loader import InputLoader
from multi_x_serverless.routing.workflow_config import WorkflowConfig


class WorkflowLoader(InputLoader):
    _workflow_data: dict[str, Any]
    _instances_regions_and_providers: dict[str, Any]
    _home_region: str

    def __init__(self, client: RemoteClient, workflow_config: WorkflowConfig) -> None:
        super().__init__(client, WORKFLOW_INSTANCE_TABLE)

        # Parse the workflow config to get the instances, regions, and providers
        ## Get the starthop or home region for the workflow
        self._home_region: str = workflow_config.home_region

        ## Get the enabled providers for each instance
        self._instances_regions_and_providers = {}
        instances_data: dict[str, dict[str, Any]] = workflow_config.instances
        for instance in instances_data.values():
            providers = instance.get("regions_and_providers", {}).get("providers", {})
            self._instances_regions_and_providers[instance["instance_name"]] = providers

        # Caches
        self._data_transfer_size_cache: dict[str, list] = {}
        self._start_hop_size_cache: dict[str, list] = {}
        self._runtime_distribution_cache: dict[str, list] = {}
        self._start_hop_latency_distribution_cache: dict[str, list] = {}

    def setup(self, workflow_id: str) -> None:
        self._workflow_data = self._retrieve_workflow_data(workflow_id)

    def get_home_region(self) -> str:
        return self._home_region

    def get_runtime_distribution(self, instance_name: str, region_name: str) -> list[float]:
        return (
            self._workflow_data.get("instance_summary", {})
            .get(instance_name, {})
            .get("executions", {})
            .get(region_name, [])
        )

    def get_start_hop_size_distribution(self, from_region_name: str) -> list[float]:
        if from_region_name in self._start_hop_size_cache:
            return self._start_hop_size_cache[from_region_name]
        resulting_size = [
            float(size) for size in self._workflow_data.get("start_hop_summary", {}).get(from_region_name, {}).keys()
        ]
        self._start_hop_size_cache[from_region_name] = resulting_size
        return resulting_size

    def get_start_hop_latency_distribution(self, from_region_name: str, data_transfer_size: float) -> list[float]:
        cache_key = f"{from_region_name}_{data_transfer_size}"
        if cache_key in self._start_hop_latency_distribution_cache:
            return self._start_hop_latency_distribution_cache[cache_key]
        start_hop_latency_distribution = (
            self._workflow_data.get("start_hop_summary", {})
            .get(from_region_name, {})
            .get(str(data_transfer_size), [SOLVER_INPUT_TRANSMISSION_LATENCY_DEFAULT])
        )
        self._start_hop_latency_distribution_cache[cache_key] = start_hop_latency_distribution
        return start_hop_latency_distribution

    def get_data_transfer_size_distribution(
        self,
        from_instance_name: str,
        to_instance_name: str,
        from_region_name: str,
        to_region_name: str,
    ) -> list[float]:
        cache_key = f"{from_instance_name}_{to_instance_name}_{from_region_name}_{to_region_name}"
        if cache_key in self._data_transfer_size_cache:
            return self._data_transfer_size_cache[cache_key]
        resulting_size = [
            float(size)
            for size in self._workflow_data.get("instance_summary", {})
            .get(from_instance_name, {})
            .get("to_instance", {})
            .get(to_instance_name, {})
            .get("regions_to_regions", {})
            .get(from_region_name, {})
            .get(to_region_name, {})
            .get("transfer_sizes", [])
        ]
        self._data_transfer_size_cache[cache_key] = resulting_size
        return resulting_size

    def get_latency_distribution(
        self,
        from_instance_name: str,
        to_instance_name: str,
        from_region_name: str,
        to_region_name: str,
        data_transfer_size: float,
    ) -> list[float]:
        return (
            self._workflow_data.get("instance_summary", {})
            .get(from_instance_name, {})
            .get("to_instance", {})
            .get(to_instance_name, {})
            .get("regions_to_regions", {})
            .get(from_region_name, {})
            .get(to_region_name, {})
            .get("transfer_size_to_transfer_latencies", {})
            .get(str(data_transfer_size), [SOLVER_INPUT_TRANSMISSION_LATENCY_DEFAULT])
        )

    def get_invocation_probability(self, from_instance_name: str, to_instance_name: str) -> float:
        if from_instance_name == to_instance_name:  # Special case for start node
            return 1

        return (
            self._workflow_data.get("instance_summary", {})
            .get(from_instance_name, {})
            .get("to_instance", {})
            .get(to_instance_name, {})
            .get("invocation_probability", SOLVER_INPUT_INVOCATION_PROBABILITY_DEFAULT)
        )

    def get_vcpu(self, instance_name: str, provider_name: str) -> float:
        vcpu = (
            self._instances_regions_and_providers.get(instance_name, {})
            .get(provider_name, {})
            .get("config", {})
            .get("vcpu", SOLVER_INPUT_VCPU_DEFAULT)
        )

        if vcpu < 0:
            # Configure memory and vcpu configuration and or translation
            if provider_name == Provider.AWS.value:
                # vcpu ratio (assuming linear, intercept at 0 scaling)
                # for aws lambda https://docs.aws.amazon.com/lambda/latest/dg/configuration-function-common.html
                vcpu = self.get_memory(instance_name, provider_name) / 1792
            else:
                raise ValueError(
                    f"vCPU count for instance {instance_name} in provider {provider_name} is not available"
                )

        return vcpu

    def get_memory(self, instance_name: str, provider_name: str) -> float:
        return (
            self._instances_regions_and_providers.get(instance_name, {})
            .get(provider_name, {})
            .get("config", {})
            .get("memory")
        )  # Memory MUST exist for a valid workflow

    def get_architecture(self, instance_name: str, provider_name: str) -> str:
        return (
            self._instances_regions_and_providers.get(instance_name, {})
            .get(provider_name, {})
            .get("config", {})
            .get("architecture", SOLVER_INPUT_ARCHITECTURE_DEFAULT)
        )  # Default to x86_64

    def _retrieve_workflow_data(self, workflow_id: str) -> dict[str, Any]:
        return self._retrieve_data(self._primary_table, workflow_id)
