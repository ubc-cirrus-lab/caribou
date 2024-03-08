import json
import logging
import re
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any, Optional

from multi_x_serverless.common.constants import (
    DEPLOYMENT_MANAGER_RESOURCE_TABLE,
    FORGETTING_NUMBER,
    FORGETTING_TIME,
    GLOBAL_TIME_ZONE,
    LOG_VERSION,
    WORKFLOW_SUMMARY_TABLE,
)
from multi_x_serverless.common.models.endpoints import Endpoints
from multi_x_serverless.common.models.remote_client.remote_client import RemoteClient
from multi_x_serverless.common.models.remote_client.remote_client_factory import RemoteClientFactory

logger = logging.getLogger(__name__)


class LogSyncer:
    def __init__(self) -> None:
        self.endpoints = Endpoints()
        self._region_clients: dict[tuple[str, str], RemoteClient] = {}

    def sync(self) -> None:
        currently_deployed_workflows = self.endpoints.get_deployment_manager_client().get_all_values_from_table(
            DEPLOYMENT_MANAGER_RESOURCE_TABLE
        )

        for workflow_id, deployment_manager_config_json in currently_deployed_workflows.items():
            logging.info("Processing workflow: %s", workflow_id)
            if not isinstance(deployment_manager_config_json, str):
                raise RuntimeError(
                    f"The deployment manager resource value for workflow_id: {workflow_id} is not a string"
                )
            self.process_workflow(workflow_id, deployment_manager_config_json)

    def process_workflow(self, workflow_id: str, deployment_manager_config_json: str) -> None:
        workflow_summary_instance = self._initialize_workflow_summary_instance()

        now_minus_forgetting_time = datetime.now(GLOBAL_TIME_ZONE) - timedelta(seconds=FORGETTING_TIME)

        deployment_manager_config = json.loads(deployment_manager_config_json)
        self._validate_deployment_manager_config(deployment_manager_config, workflow_id)

        deployed_region_json = deployment_manager_config.get("deployed_regions", "{}")
        deployed_region: dict[str, dict[str, Any]] = json.loads(deployed_region_json)

        latency_summary: dict[str, dict[str, dict[str, dict[str, Any]]]] = defaultdict(
            lambda: defaultdict(lambda: defaultdict(dict))
        )
        latency_summary_successor_before_caller_store: dict[str, dict[str, dict[str, Any]]] = defaultdict(
            lambda: defaultdict(dict)
        )

        for function_physical_instance, instance_information in deployed_region.items():
            provider_region = instance_information["deploy_region"]
            self._process_function_instance(
                function_physical_instance,
                provider_region,
                workflow_summary_instance,
                now_minus_forgetting_time,
                latency_summary,  # type: ignore
                latency_summary_successor_before_caller_store,  # type: ignore
            )

        latency_aggregates = self._cleanup_latency_summary(
            latency_summary, latency_summary_successor_before_caller_store  # type: ignore
        )

        self._update_workflow_summary_with_latency_summary(workflow_summary_instance, latency_aggregates)

        workflow_summary_instance_json = json.dumps(workflow_summary_instance)

        self.endpoints.get_datastore_client().update_value_in_table(
            WORKFLOW_SUMMARY_TABLE,
            workflow_id,
            workflow_summary_instance_json,
        )

    def _update_workflow_summary_with_latency_summary(
        self,
        workflow_summary_instance: dict[str, Any],
        latency_aggregates: dict[str, dict[str, dict[str, dict[str, Any]]]],
    ) -> None:
        for caller_instance, caller_data in workflow_summary_instance["instance_summary"].items():
            for callee_instance, outgoing_providers in caller_data["invocation_summary"].items():
                outgoing_providers["transmission_summary"] = latency_aggregates[caller_instance][callee_instance]

    def _cleanup_latency_summary(
        self,
        latency_summary: dict[str, dict[str, dict[str, dict[str, Any]]]],
        latency_summary_successor_before_caller_store: dict[str, dict[str, dict[str, Any]]],
    ) -> dict[str, dict[str, dict[str, dict[str, Any]]]]:
        for run_id, caller_data in latency_summary.items():
            for _, successor_latency in caller_data.items():
                for successor_instance, latency in successor_latency.items():
                    if latency["end_time"] is None:
                        if (
                            run_id in latency_summary_successor_before_caller_store
                            and successor_instance in latency_summary_successor_before_caller_store[run_id]
                        ):
                            latency["end_time"] = latency_summary_successor_before_caller_store[run_id][
                                successor_instance
                            ]["end_time"]
                            latency["incoming_provider"] = latency_summary_successor_before_caller_store[run_id][
                                successor_instance
                            ]["incoming_provider"]

        aggregated_data: dict[str, dict[str, dict[str, dict[str, Any]]]] = {}

        # Step 1: Aggregate the latencies
        for run_id, caller_data in latency_summary.items():
            for caller_id, callee_data in caller_data.items():
                for callee_id, details in callee_data.items():
                    outgoing_provider = details["outgoing_provider"]
                    incoming_provider = details["incoming_provider"]
                    latency = details["end_time"] - details["start_time"]

                    caller_agg = aggregated_data.setdefault(caller_id, {})
                    callee_agg = caller_agg.setdefault(callee_id, {})
                    outgoing_agg = callee_agg.setdefault(outgoing_provider, {})
                    incoming_agg = outgoing_agg.setdefault(
                        incoming_provider, {"latency_samples": [], "transmission_count": 0}
                    )

                    incoming_agg["latency_samples"].append(latency)
                    incoming_agg["transmission_count"] += 1

        return aggregated_data

    def _initialize_workflow_summary_instance(self) -> dict[str, Any]:
        workflow_summary_instance: dict[str, Any] = {}
        workflow_summary_instance["instance_summary"] = {}
        workflow_summary_instance["total_invocations"] = 0
        return workflow_summary_instance

    def _validate_deployment_manager_config(self, deployment_manager_config: dict[str, Any], workflow_id: str) -> None:
        if "deployed_regions" not in deployment_manager_config:
            raise RuntimeError(
                f"deployed_regions not found in deployment_manager_config for workflow_id: {workflow_id}"
            )

    def _process_function_instance(
        self,
        function_instance: str,
        provider_region: dict[str, str],
        workflow_summary_instance: dict[str, Any],
        time_window_start: datetime,
        latency_summary: dict[str, dict[str, dict[str, dict[str, Any]]]],
        latency_summary_successor_before_caller_store: dict[str, dict[str, dict[str, Any]]],
    ) -> None:
        logger.info("Processing function instance: %s", function_instance)
        if (provider_region["provider"], provider_region["region"]) not in self._region_clients:
            self._region_clients[
                (provider_region["provider"], provider_region["region"])
            ] = RemoteClientFactory.get_remote_client(provider_region["provider"], provider_region["region"])

        logs: list[str] = self._region_clients[(provider_region["provider"], provider_region["region"])].get_logs_since(
            function_instance, time_window_start
        )

        self._trim_logs(logs)

        self._process_logs(
            logs,
            provider_region,
            workflow_summary_instance,
            latency_summary,
            latency_summary_successor_before_caller_store,
        )

    def _trim_logs(self, logs: list[str]) -> None:
        # The logs from provider are in chronological order, so we can remove the first few logs
        # to forget the old logs
        number_of_entry_point_logs = 0
        for i in reversed(range(len(logs))):
            if "ENTRY_POINT" in logs[i]:
                number_of_entry_point_logs += 1
                if number_of_entry_point_logs > FORGETTING_NUMBER:
                    del logs[: i + 1]
                    break

    def _initialize_instance_summary(
        self,
        function_instance: str,
        provider_region: dict[str, str],
        workflow_summary_instance: dict[str, Any],
        entry_point: bool = False,
    ) -> None:
        if function_instance not in workflow_summary_instance["instance_summary"]:
            workflow_summary_instance["instance_summary"][function_instance] = {
                "invocation_count": 0,
                "execution_summary": {
                    f"{provider_region['provider']}:{provider_region['region']}": {
                        "invocation_count": 0,
                        "runtime_samples": [],
                    }
                },
                "invocation_summary": {},
            }
        elif (
            f"{provider_region['provider']}:{provider_region['region']}"
            not in workflow_summary_instance["instance_summary"][function_instance]["execution_summary"]
        ):
            workflow_summary_instance["instance_summary"][function_instance]["execution_summary"][
                f"{provider_region['provider']}:{provider_region['region']}"
            ] = {
                "invocation_count": 0,
                "runtime_samples": [],
            }

        if entry_point:
            if (
                "init_data_transfer_size_samples"
                not in workflow_summary_instance["instance_summary"][function_instance]["execution_summary"][
                    f"{provider_region['provider']}:{provider_region['region']}"
                ]
            ):
                workflow_summary_instance["instance_summary"][function_instance]["execution_summary"][
                    f"{provider_region['provider']}:{provider_region['region']}"
                ]["init_data_transfer_size_samples"] = []
            if (
                "init_latency_samples"
                not in workflow_summary_instance["instance_summary"][function_instance]["execution_summary"][
                    f"{provider_region['provider']}:{provider_region['region']}"
                ]
            ):
                workflow_summary_instance["instance_summary"][function_instance]["execution_summary"][
                    f"{provider_region['provider']}:{provider_region['region']}"
                ]["init_latency_samples"] = []

    def _extract_invoked_logs(
        self,
        log_entry: str,
        provider_region: dict[str, str],
        workflow_summary_instance: dict[str, Any],
        latency_summary: dict[str, dict[str, dict[str, dict[str, Any]]]],
        latency_summary_successor_before_caller_store: dict[str, dict[str, dict[str, Any]]],
        run_id: str,
        end_time: float,
    ) -> None:
        function_invoked = self._extract_from_string(log_entry, r"INSTANCE \((.*?)\)")
        if not isinstance(function_invoked, str):
            return
        self._initialize_instance_summary(function_invoked, provider_region, workflow_summary_instance)
        workflow_summary_instance["instance_summary"][function_invoked]["invocation_count"] += 1
        workflow_summary_instance["instance_summary"][function_invoked]["execution_summary"][
            f"{provider_region['provider']}:{provider_region['region']}"
        ]["invocation_count"] += 1
        self._update_invoked_latency_summary(
            run_id,
            function_invoked,
            end_time,
            latency_summary,
            latency_summary_successor_before_caller_store,
            provider_region,
        )

    def _update_invoked_latency_summary(
        self,
        run_id: str,
        instance_name: str,
        end_time: float,
        latency_summary: dict[str, dict[str, dict[str, dict[str, Any]]]],
        latency_summary_successor_before_caller_store: dict[str, dict[str, dict[str, Any]]],
        provider_region: dict[str, str],
    ) -> None:
        found = False
        provider_region_str = f"{provider_region['provider']}:{provider_region['region']}"
        for _, functions in latency_summary.items():
            for _, successors in functions.items():
                for callee_instance, latency in successors.items():
                    if callee_instance == instance_name:
                        # Found the corresponding successor entry, update end_time
                        latency["end_time"] = end_time
                        latency["incoming_provider"] = provider_region_str
                        found = True
                        break

        if not found:
            # If the corresponding successor entry was not found,
            # store the end_time in latency_summary_successor_before_caller_store
            latency_summary_successor_before_caller_store[run_id][instance_name] = {
                "end_time": end_time,
                "incoming_provider": provider_region_str,
            }

    def _update_invoking_successor_latency_summary(
        self,
        run_id: str,
        instance_name: str,
        successor_name: str,
        start_time: float,
        latency_summary: dict[str, dict[str, dict[str, dict[str, Any]]]],
        provider_region: dict[str, str],
    ) -> None:
        # Ensure the structure exists
        run_entry = latency_summary.setdefault(run_id, {})
        instance_entry = run_entry.setdefault(instance_name, {})
        successor_entry = instance_entry.setdefault(
            successor_name, {"start_time": None, "end_time": None, "outgoing_provider": None, "incoming_provider": None}
        )

        # Only update start_time if it's not already set (to handle out-of-order logs)
        if successor_entry["start_time"] is None:
            successor_entry["start_time"] = start_time
            provider_region_str = f"{provider_region['provider']}:{provider_region['region']}"
            successor_entry["outgoing_provider"] = provider_region_str

    def _extract_executed_logs(self, log_entry: str, runtimes: dict[str, list[float]]) -> None:
        function_executed = self._extract_from_string(log_entry, r"INSTANCE \((.*?)\)")
        if not isinstance(function_executed, str):
            return
        duration = self._extract_from_string(log_entry, r"EXECUTION_TIME \((.*?)\)")
        if duration:
            duration = float(duration)  # type: ignore
        if not isinstance(duration, float):
            return
        if function_executed not in runtimes:
            runtimes[function_executed] = []
        runtimes[function_executed].append(duration)

    def _extract_from_string(self, log_entry: str, regex: str) -> Optional[str]:
        match = re.search(regex, log_entry)
        return match.group(1) if match else None

    def _extract_invoking_successor_logs(
        self,
        log_entry: str,
        provider_region: dict[str, str],
        workflow_summary_instance: dict[str, Any],
        data_transfer_sizes: dict[str, dict[str, list[float]]],
        latency_summary: dict[str, dict[str, dict[str, dict[str, Any]]]],
        run_id: str,
        start_time: float,
    ) -> None:
        caller_function = self._extract_from_string(log_entry, r"INSTANCE \((.*?)\)")
        if not isinstance(caller_function, str):
            return
        self._initialize_instance_summary(caller_function, provider_region, workflow_summary_instance)
        successor_function = self._extract_from_string(log_entry, r"SUCCESSOR \((.*?)\)")
        if not isinstance(successor_function, str):
            return

        self._update_invoking_successor_latency_summary(
            run_id, caller_function, successor_function, start_time, latency_summary, provider_region
        )
        if (
            successor_function
            not in workflow_summary_instance["instance_summary"][caller_function]["invocation_summary"]
        ):
            workflow_summary_instance["instance_summary"][caller_function]["invocation_summary"][successor_function] = {
                "invocation_count": 0,
                "data_transfer_samples": [],
                "transmission_summary": {},
            }
        workflow_summary_instance["instance_summary"][caller_function]["invocation_summary"][successor_function][
            "invocation_count"
        ] += 1
        if caller_function not in data_transfer_sizes:
            data_transfer_sizes[caller_function] = {}
        if successor_function not in data_transfer_sizes[caller_function]:
            data_transfer_sizes[caller_function][successor_function] = []
        data_transfer_size = self._extract_from_string(log_entry, r"PAYLOAD_SIZE \((.*?)\)")
        if data_transfer_size:
            data_transfer_size = float(data_transfer_size)  # type: ignore
        if not isinstance(data_transfer_size, float):
            return
        data_transfer_sizes[caller_function][successor_function].append(data_transfer_size)

    def _extract_entry_point_log(
        self, log_entry: str, workflow_summary_instance: dict[str, Any], provider_region: dict[str, str]
    ) -> None:
        workflow_summary_instance["total_invocations"] += 1
        function_invoked = self._extract_from_string(log_entry, r"INSTANCE \((.*?)\)")
        if not isinstance(function_invoked, str):
            return
        self._initialize_instance_summary(
            function_invoked, provider_region, workflow_summary_instance, entry_point=True
        )
        data_transfer_size = self._extract_from_string(log_entry, r"PAYLOAD_SIZE \((.*?)\)")
        if data_transfer_size:
            data_transfer_size_fl = float(data_transfer_size)
            workflow_summary_instance["instance_summary"][function_invoked]["execution_summary"][
                f"{provider_region['provider']}:{provider_region['region']}"
            ]["init_data_transfer_size_samples"].append(data_transfer_size_fl)
        init_latency = self._extract_from_string(log_entry, r"INIT_LATENCY \((.*?)\)")
        if init_latency and init_latency != "N/A":
            init_latency_fl = float(init_latency)
            workflow_summary_instance["instance_summary"][function_invoked]["execution_summary"][
                f"{provider_region['provider']}:{provider_region['region']}"
            ]["init_latency_samples"].append(init_latency_fl)

    def _process_logs(
        self,
        logs: list[str],
        provider_region: dict[str, str],
        workflow_summary_instance: dict[str, Any],
        latency_summary: dict[str, dict[str, dict[str, dict[str, Any]]]],
        latency_summary_successor_before_caller_store: dict[str, dict[str, dict[str, Any]]],
    ) -> None:
        data_transfer_sizes: dict[str, dict[str, list[float]]] = {}
        runtimes: dict[str, list[float]] = {}

        for log_entry in logs:
            if f"LOG_VERSION ({LOG_VERSION})" not in log_entry:
                continue
            run_id = self._extract_from_string(log_entry, r"RUN_ID \((.*?)\)")
            if not isinstance(run_id, str):
                continue
            log_time = self._extract_from_string(log_entry, r"TIME \((.*?)\)")
            if log_time:
                log_time = datetime.strptime(log_time, "%Y-%m-%d %H:%M:%S,%f").timestamp()  # type: ignore
            if not isinstance(log_time, float):
                continue
            if "ENTRY_POINT" in log_entry:
                self._extract_entry_point_log(log_entry, workflow_summary_instance, provider_region)
            if "INVOKED" in log_entry:
                self._extract_invoked_logs(
                    log_entry,
                    provider_region,
                    workflow_summary_instance,
                    latency_summary,
                    latency_summary_successor_before_caller_store,
                    run_id,
                    log_time,
                )
            if "EXECUTED" in log_entry:
                self._extract_executed_logs(log_entry, runtimes)
            if "INVOKING_SUCCESSOR" in log_entry:
                self._extract_invoking_successor_logs(
                    log_entry,
                    provider_region,
                    workflow_summary_instance,
                    data_transfer_sizes,
                    latency_summary,
                    run_id,
                    log_time,
                )

        for caller_function_instance, successor_functions_instances in data_transfer_sizes.items():
            for successor_function_instance, data_transfer_size_list in successor_functions_instances.items():
                workflow_summary_instance["instance_summary"][caller_function_instance]["invocation_summary"][
                    successor_function_instance
                ]["data_transfer_samples"].extend(data_transfer_size_list)

        for function_instance_str, runtimes_list in runtimes.items():
            workflow_summary_instance["instance_summary"][function_instance_str]["execution_summary"][
                f"{provider_region['provider']}:{provider_region['region']}"
            ]["runtime_samples"].extend(runtimes_list)
