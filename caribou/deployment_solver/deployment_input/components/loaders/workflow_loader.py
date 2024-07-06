import math
from typing import Any, Optional

from caribou.common.constants import (  # SOLVER_INPUT_TRANSMISSION_LATENCY_DEFAULT,
    SNS_SIZE_DEFAULT,
    SOLVER_INPUT_ARCHITECTURE_DEFAULT,
    SOLVER_INPUT_INVOCATION_PROBABILITY_DEFAULT,
    SOLVER_INPUT_VCPU_DEFAULT,
    SYNC_SIZE_DEFAULT,
    WORKFLOW_INSTANCE_TABLE,
)
from caribou.common.models.remote_client.remote_client import RemoteClient
from caribou.common.provider import Provider
from caribou.deployment_solver.deployment_input.components.loader import InputLoader
from caribou.deployment_solver.workflow_config import WorkflowConfig


# pylint: disable=too-many-public-methods
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
        # self._start_hop_size_cache: dict[str, list] = {}
        self._runtime_distribution_cache: dict[str, list] = {}
        self._start_hop_latency_distribution_cache: dict[str, list] = {}

    def setup(self, workflow_id: str) -> None:
        self._workflow_data = self._retrieve_workflow_data(workflow_id)

    def get_workflow_data(self) -> dict[str, Any]:
        return self._workflow_data

    def set_workflow_data(self, data: dict[str, Any]) -> None:
        self._workflow_data = data

    def get_home_region(self) -> str:
        return self._home_region

    def get_wpd_size(self) -> list[float]:
        # Workflow Placement Decision Size
        return self._workflow_data.get("start_hop_summary", {}).get("workflow_placement_decision_size_gb", 0.0)

    def get_start_hop_size_distribution(self) -> list[float]:
        # Start hop size distribution, if not available, return the WPD size
        # As it will always send at least the WPD size
        return self._workflow_data.get("start_hop_summary", {}).get("transfer_sizes_gb", [self.get_wpd_size()])

    def get_start_hop_best_fit_line(self, to_region_name: str) -> Optional[dict[str, float]]:
        best_fit_line = (
            self._workflow_data.get("start_hop_summary", {})
            .get("regions_to_regions", {})
            .get(to_region_name, {})
            .get("best_fit_line", None)
        )
        return best_fit_line

    def get_start_hop_latency_distribution(self, to_region_name: str, data_transfer_size: float) -> list[float]:
        cache_key = f"{to_region_name}_{data_transfer_size}"
        if cache_key in self._start_hop_latency_distribution_cache:
            return self._start_hop_latency_distribution_cache[cache_key]

        # Round data transfer size translation to nearest 10 KB
        data_transfer_size = self._round_to_kb(data_transfer_size, 10)

        start_hop_latency_distribution = (
            self._workflow_data.get("start_hop_summary", {})
            .get("regions_to_regions", {})
            .get(to_region_name, {})
            .get("transfer_size_gb_to_transfer_latencies_s", {})
            .get(str(data_transfer_size), [])
        )

        if len(start_hop_latency_distribution) == 0:
            # Atempt to use the best fit line size
            best_fit_line = self.get_start_hop_best_fit_line(to_region_name)
            if best_fit_line is not None and best_fit_line != {}:
                # Estimate the latency using the best fit line
                estimated_latency = best_fit_line["slope_s"] * data_transfer_size + best_fit_line["intercept_s"]

                # Limit the estimated latency to the min and max latency
                estimated_latency = min(
                    best_fit_line["max_latency_s"], max(best_fit_line["min_latency_s"], estimated_latency)
                )

                start_hop_latency_distribution = [estimated_latency]

        self._start_hop_latency_distribution_cache[cache_key] = start_hop_latency_distribution
        return start_hop_latency_distribution

    def get_average_cpu_utilization(self, instance_name: str) -> float:
        # Get the average CPU utilization for the instance
        # If not available, default to 0.5 (Average cpu utilization of
        # hyperscale cloud providers)
        return self._workflow_data.get("instance_summary", {}).get(instance_name, {}).get("cpu_utilization", 0.5)

    def get_runtime_distribution(self, instance_name: str, region_name: str) -> list[float]:
        return (
            self._workflow_data.get("instance_summary", {})
            .get(instance_name, {})
            .get("executions", {})
            .get("at_region", {})
            .get(region_name, {})
            .get("durations_s", [])
        )

    def get_auxiliary_data_distribution(
        self, instance_name: str, region_name: str, runtime: float
    ) -> list[list[float]]:
        # Round the duration to the nearest 10 ms
        runtime = self._round_to_ms(runtime, 10)

        auxiliary_data_distribution: list[list[float]] = (
            self._workflow_data.get("instance_summary", {})
            .get(instance_name, {})
            .get("executions", {})
            .get("at_region", {})
            .get(region_name, {})
            .get("auxiliary_data", {})
            .get(str(runtime), [])
        )

        return auxiliary_data_distribution

    def get_auxiliary_index_translation(self, instance_name: str) -> dict[str, int]:
        auxiliary_index_translation: dict[str, int] = (
            self._workflow_data.get("instance_summary", {})
            .get(instance_name, {})
            .get("executions", {})
            .get("auxiliary_index_translation", {})
        )

        return auxiliary_index_translation

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

    def get_data_transfer_size_distribution(
        self,
        from_instance_name: str,
        to_instance_name: str,
    ) -> list[float]:
        cache_key = f"{from_instance_name}_{to_instance_name}"
        if cache_key in self._data_transfer_size_cache:
            return self._data_transfer_size_cache[cache_key]

        resulting_size = [
            float(size)
            for size in self._workflow_data.get("instance_summary", {})
            .get(from_instance_name, {})
            .get("to_instance", {})
            .get(to_instance_name, {})
            .get("transfer_sizes_gb", [])
        ]

        self._data_transfer_size_cache[cache_key] = resulting_size
        return resulting_size

    def get_latency_distribution_best_fit_line(
        self, from_instance_name: str, to_instance_name: str, from_region_name: str, to_region_name: str
    ) -> Optional[dict[str, float]]:
        best_fit_line = (
            self._workflow_data.get("instance_summary", {})
            .get(from_instance_name, {})
            .get("to_instance", {})
            .get(to_instance_name, {})
            .get("regions_to_regions", {})
            .get(from_region_name, {})
            .get(to_region_name, {})
            .get("best_fit_line", None)
        )
        return best_fit_line

    def get_latency_distribution(
        self,
        from_instance_name: str,
        to_instance_name: str,
        from_region_name: str,
        to_region_name: str,
        data_transfer_size: float,
    ) -> list[float]:
        # Round data transfer size translation to nearest 10 KB
        data_transfer_size = self._round_to_kb(data_transfer_size, 10)

        latency_distribution = (
            self._workflow_data.get("instance_summary", {})
            .get(from_instance_name, {})
            .get("to_instance", {})
            .get(to_instance_name, {})
            .get("regions_to_regions", {})
            .get(from_region_name, {})
            .get(to_region_name, {})
            .get("transfer_size_gb_to_transfer_latencies_s", {})
            .get(str(data_transfer_size), [])
        )

        if len(latency_distribution) == 0:
            # Attempt to use the best fit line size
            best_fit_line = self.get_latency_distribution_best_fit_line(
                from_instance_name, to_instance_name, from_region_name, to_region_name
            )
            if best_fit_line is not None and best_fit_line != {}:
                # Estimate the latency using the best fit line
                estimated_latency = best_fit_line["slope_s"] * data_transfer_size + best_fit_line["intercept_s"]

                # Limit the estimated latency to the min and max latency
                estimated_latency = min(
                    best_fit_line["max_latency_s"], max(best_fit_line["min_latency_s"], estimated_latency)
                )

                latency_distribution = [estimated_latency]

        return latency_distribution

    def get_non_execution_information(self, from_instance_name: str, to_instance_name: str) -> dict[str, Any]:
        # Should return only the name of each entry of non_execution_info
        # And the sync_data_response_size_gb
        non_execution_info_dict: dict[str, float] = {}
        for key, value in (
            self._workflow_data.get("instance_summary", {})
            .get(from_instance_name, {})
            .get("to_instance", {})
            .get(to_instance_name, {})
            .get("non_execution_info", {})
            .items()
        ):
            non_execution_info_dict[key] = value.get("sync_data_response_size_gb", 0.0)

        return non_execution_info_dict

    def get_non_execution_sns_transfer_size(
        self, from_instance_name: str, to_instance_name: str, sync_to_from_instance: str
    ) -> float:
        # Round to the nearest non-zero KB
        # (At least 1 byte of data is transferred for sns)
        return self._round_to_kb(
            (
                self._workflow_data.get("instance_summary", {})
                .get(from_instance_name, {})
                .get("to_instance", {})
                .get(to_instance_name, {})
                .get("non_execution_info", {})
                .get(sync_to_from_instance, {})
                .get("sns_transfer_size_gb", 0.0)
            ),
            1,
            False,
        )

    def get_non_execution_transfer_latency_distribution(
        self,
        from_instance_name: str,
        to_instance_name: str,
        sync_to_from_instance: str,
        from_region_name: str,
        to_region_name: str,
    ) -> list[float]:
        return (
            self._workflow_data.get("instance_summary", {})
            .get(from_instance_name, {})
            .get("to_instance", {})
            .get(to_instance_name, {})
            .get("non_execution_info", {})
            .get(sync_to_from_instance, {})
            .get("regions_to_regions", {})
            .get(from_region_name, {})
            .get(to_region_name, {})
            .get("transfer_latencies_s", [])
        )

    def get_sync_size(self, from_instance_name: str, to_instance_name: str) -> float:
        return (
            self._workflow_data.get("instance_summary", {})
            .get(from_instance_name, {})
            .get("to_instance", {})
            .get(to_instance_name, {})
            .get("sync_sizes_gb", SYNC_SIZE_DEFAULT)
        )

    def get_sns_only_size(self, from_instance_name: str, to_instance_name: str) -> float:
        return (
            self._workflow_data.get("instance_summary", {})
            .get(from_instance_name, {})
            .get("to_instance", {})
            .get(to_instance_name, {})
            .get("sns_only_sizes_gb", SNS_SIZE_DEFAULT)
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
                # for aws lambda https://docs.aws.amazon.com/lambda/latest/dg/configuration-memory.html
                vcpu = self.get_memory(instance_name, provider_name) / 1769
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

    def _round_to_kb(self, number: float, round_to: int = 10, round_up: bool = True) -> float:
        """
        Rounds the input number (in GB) to the nearest KB or 10 KB in base 2, rounding up
        or to the nearest non_zero.

        :param number: The input number in GB.
        :param round_to: The value to round to (1 for nearest KB, 10 for nearest 10 KB).
        :param round_up: Whether to round up or to nearest non-zero KB.
        :return: The rounded number in GB.
        """
        rounded_kb = number * (1024**2) / round_to
        if round_up:
            rounded_kb = math.ceil(rounded_kb)
        else:
            # Round to the nearest non-zero
            rounded_kb = math.floor(rounded_kb + 0.5)
            if rounded_kb == 0:
                rounded_kb = 1

        return rounded_kb * round_to / (1024**2)
        # return math.ceil(number * (1024**2) / round_to) * round_to / (1024**2)

    def _round_to_ms(self, number: float, round_to: int = 1, round_up: bool = True) -> float:
        """
        Rounds the input number (in seconds) to the nearest ms, rounding up
        or to the nearest non_zero.

        :param number: The input number in seconds.
        :param round_to: The value to round to (1 for nearest ms, 10 for nearest 10 ms).
        :param round_up: Whether to round up or to nearest non-zero ms.
        :return: The rounded number in seconds.
        """
        # return math.ceil(number * 1000 / round_to) * round_to / 1000

        rounded_ms = number * 1000 / round_to
        if round_up:
            rounded_ms = math.ceil(rounded_ms)
        else:
            # Round to the nearest non-zero
            rounded_ms = math.floor(rounded_ms + 0.5)
            if rounded_ms == 0:
                rounded_ms = 1

        return rounded_ms * round_to / 1000

    def toDict(self):
        return {
            "workflow_data": self._workflow_data,
            "instances_regions_and_providers": self._instances_regions_and_providers,
            "home_region": self._home_region,
        }
