from typing import Any, Optional

from multi_x_serverless.common.constants import (
    SOLVER_INPUT_ARCHITECTURE_DEFAULT,
    SOLVER_INPUT_DATA_TRANSFER_SIZE_DEFAULT,
    SOLVER_INPUT_INVOCATION_PROBABILITY_DEFAULT,
    SOLVER_INPUT_LATENCY_DEFAULT,
    SOLVER_INPUT_PROJECTED_MONTHLY_INVOCATIONS_DEFAULT,
    SOLVER_INPUT_RUNTIME_DEFAULT,
    SOLVER_INPUT_VCPU_DEFAULT,
    WORKFLOW_INSTANCE_TABLE,
)
from multi_x_serverless.common.models.remote_client.remote_client import RemoteClient
from multi_x_serverless.common.provider import Provider
from multi_x_serverless.routing.solver.input.components.loader import InputLoader


class WorkflowLoader(InputLoader):
    _workflow_data: dict[str, Any]
    _instances_regions_and_providers: dict[str, Any]

    def __init__(self, client: RemoteClient, instances_data: list[dict]) -> None:
        super().__init__(client, WORKFLOW_INSTANCE_TABLE)

        self._instances_regions_and_providers = {}
        for instance in instances_data:
            providers = instance.get("regions_and_providers", {}).get("providers", {})
            self._instances_regions_and_providers[instance["instance_name"]] = providers

    def setup(self, workflow_id: str) -> None:
        self._workflow_data = self._retrieve_workflow_data(workflow_id)

    def get_runtime(self, instance_name: str, region_name: str, use_tail_runtime: bool = False) -> float:
        runtime_type = "tail_runtime" if use_tail_runtime else "average_runtime"
        return (
            self._workflow_data.get(instance_name, {})
            .get("execution_summary", {})
            .get(region_name, {})
            .get(runtime_type, SOLVER_INPUT_RUNTIME_DEFAULT)
        )

    def get_latency(
        self,
        from_instance_name: str,
        to_instance_name: str,
        from_region_name: str,
        to_region_name: str,
        use_tail_runtime: bool = False,
    ) -> float:
        latency_type = "tail_latency" if use_tail_runtime else "average_latency"
        return (
            self._workflow_data.get(from_instance_name, {})
            .get("invocation_summary", {})
            .get(to_instance_name, {})
            .get("transmission_summary", {})
            .get(from_region_name, {})
            .get(to_region_name, {})
            .get(latency_type, SOLVER_INPUT_LATENCY_DEFAULT)
        )

    def get_data_transfer_size(self, from_instance_name: str, to_instance_name: str) -> float:
        return (
            self._workflow_data.get(from_instance_name, {})
            .get("invocation_summary", {})
            .get(to_instance_name, {})
            .get("average_data_transfer_size", SOLVER_INPUT_DATA_TRANSFER_SIZE_DEFAULT)
        )

    def get_invocation_probability(self, from_instance_name: str, to_instance_name: str) -> float:
        if from_instance_name == to_instance_name:  # Special case for start node
            return 1

        return (
            self._workflow_data.get(from_instance_name, {})
            .get("invocation_summary", {})
            .get(to_instance_name, {})
            .get("probability_of_invocation", SOLVER_INPUT_INVOCATION_PROBABILITY_DEFAULT)
        )

    def get_favourite_region(self, instance_name: str) -> Optional[str]:
        return self._workflow_data.get(instance_name, {}).get("favourite_home_region", None)

    def get_favourite_region_runtime(self, instance_name: str, use_tail_runtime: bool = False) -> float:
        runtime_type = (
            "favourite_home_region_tail_runtime" if use_tail_runtime else "favourite_home_region_average_runtime"
        )

        # This instance MUST exist in the workflow data for this to ever be called
        return self._workflow_data.get(instance_name, {}).get(runtime_type)

    def get_all_favorite_regions(self) -> set[str]:
        return {instance["favourite_home_region"] for instance in self._workflow_data.values()}

    def get_projected_monthly_invocations(self, instance_name: str) -> float:
        return self._workflow_data.get(instance_name, {}).get(
            "projected_monthly_invocations", SOLVER_INPUT_PROJECTED_MONTHLY_INVOCATIONS_DEFAULT
        )

    def get_vcpu(self, instance_name: str, provider_name: str) -> int:
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

    def get_memory(self, instance_name: str, provider_name: str) -> int:
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
