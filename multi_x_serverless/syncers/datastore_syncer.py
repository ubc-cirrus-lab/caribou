import json
import re
from datetime import datetime
from typing import Any

from multi_x_serverless.common.constants import DEPLOYMENT_MANAGER_RESOURCE_TABLE, WORKFLOW_SUMMARY_TABLE
from multi_x_serverless.common.models.endpoints import Endpoints
from multi_x_serverless.common.models.remote_client.remote_client import RemoteClient
from multi_x_serverless.common.models.remote_client.remote_client_factory import RemoteClientFactory


class DatastoreSyncer:
    def __init__(self) -> None:
        self.endpoints = Endpoints()
        self._region_clients: dict[tuple[str, str], RemoteClient] = {}

    def sync(self) -> None:
        currently_deployed_workflows = self.endpoints.get_deployment_manager_client().get_all_values_from_table(
            DEPLOYMENT_MANAGER_RESOURCE_TABLE
        )

        for workflow_id, deployment_manager_config_json in currently_deployed_workflows.items():
            if not isinstance(deployment_manager_config_json, str):
                raise Exception(f"The deployment manager resource value for workflow_id: {workflow_id} is not a string")
            self.process_workflow(workflow_id, deployment_manager_config_json)

    def process_workflow(self, workflow_id: str, deployment_manager_config_json: str) -> None:
        workflow_summary_instance = self.initialize_workflow_summary_instance()

        last_synced_time = self.get_last_synced_time(workflow_id)
        workflow_summary_instance["time_since_last_sync"] = (datetime.now() - last_synced_time).total_seconds() / (
            24 * 60 * 60
        )

        deployment_manager_config = json.loads(deployment_manager_config_json)
        self.validate_deployment_manager_config(deployment_manager_config, workflow_id)

        deployed_region_json = deployment_manager_config.get("deployed_regions")
        deployed_region: dict[str, dict[str, str]] = json.loads(deployed_region_json)

        total_invocations = 0
        for function_physical_instance, provider_region in deployed_region.items():
            function_instance = "_".join(function_physical_instance.split("_")[:-1])
            total_invocations += self.process_function_instance(
                function_instance, provider_region, workflow_summary_instance, last_synced_time
            )

        workflow_summary_instance["total_invocations"] = total_invocations
        new_last_sync_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")

        workflow_summary_instance_json = json.dumps(workflow_summary_instance)

        self.endpoints.get_datastore_client().put_value_to_sort_key_table(
            WORKFLOW_SUMMARY_TABLE, workflow_id, new_last_sync_time, workflow_summary_instance_json
        )

    def initialize_workflow_summary_instance(self) -> dict[str, Any]:
        workflow_summary_instance: dict[str, Any] = {}
        workflow_summary_instance["instance_summary"] = {}
        return workflow_summary_instance

    def get_last_synced_time(self, workflow_id: str) -> datetime:
        last_synced_log = self.endpoints.get_datastore_client().get_last_value_from_sort_key_table(
            WORKFLOW_SUMMARY_TABLE, workflow_id
        )

        if last_synced_log:
            last_synced_time_str = last_synced_log[0]
            return datetime.strptime(last_synced_time_str, "%Y-%m-%d %H:%M:%S.%f")
        else:
            return datetime.now() - datetime.timedelta(days=1)

    def validate_deployment_manager_config(self, deployment_manager_config: dict[str, Any], workflow_id: str) -> None:
        if "deployed_regions" not in deployment_manager_config:
            raise Exception(f"deployed_regions not found in deployment_manager_config for workflow_id: {workflow_id}")

    def process_function_instance(
        self,
        function_instance: str,
        provider_region: dict[str, str],
        workflow_summary_instance: dict[str, Any],
        last_synced_time: datetime,
    ) -> int:
        self.initialize_instance_summary(function_instance, provider_region, workflow_summary_instance)

        if (provider_region["provider"], provider_region["region"]) not in self._region_clients:
            self._region_clients[
                (provider_region["provider"], provider_region["region"])
            ] = RemoteClientFactory.get_remote_client(provider_region["provider"], provider_region["region"])

        logs: list[str] = self._region_clients[
            (provider_region["provider"], provider_region["region"])
        ].get_logs_since_last_sync(function_instance, last_synced_time)

        return self.process_logs(logs, function_instance, provider_region, workflow_summary_instance)

    def initialize_instance_summary(
        self, function_instance: str, provider_region: dict[str, str], workflow_summary_instance: dict[str, Any]
    ) -> None:
        if function_instance not in workflow_summary_instance["instance_summary"]:
            workflow_summary_instance["instance_summary"][function_instance] = {
                "invocation_count": 0,
                "execution_summary": {
                    f"{provider_region['provider']}:{provider_region['region']}": {
                        "invocation_count": 0,
                        "average_runtime": 0,
                        "tail_runtime": 0,
                    }
                },
                "invocation_summary": {},
            }
        else:
            workflow_summary_instance["instance_summary"][function_instance]["execution_summary"][
                f"{provider_region['provider']}:{provider_region['region']}"
            ] = {
                "invocation_count": 0,
                "average_runtime": 0,
                "tail_runtime": 0,
            }

    def process_logs(
        self,
        logs: list[str],
        function_instance: str,
        provider_region: dict[str, str],
        workflow_summary_instance: dict[str, Any],
    ) -> int:
        runtimes: list[float] = []
        invocation_count = 0
        entry_point_invocation_count = 0

        data_transfer_sizes: dict[str, list[float]] = {}

        for log_entry in logs:
            if "ENTRY_POINT" in log_entry:
                entry_point_invocation_count += 1
            if "INVOKED" in log_entry:
                invocation_count += 1
            if "Billed Duration" in log_entry:
                billed_duration = re.search(r"Billed Duration: (\d+(\.\d+)?)", log_entry)
                if billed_duration:
                    runtimes.append(float(billed_duration.group(1)))
            if "INVOKING_SUCCESSOR" in log_entry:
                successor_function = re.search(r"SUCCESSOR \((.*?)\)", log_entry)
                if successor_function:
                    successor_function_str = successor_function.group(1)
                    if not isinstance(successor_function_str, str):
                        continue
                    if (
                        successor_function_str
                        not in workflow_summary_instance["instance_summary"][function_instance]["invocation_summary"]
                    ):
                        workflow_summary_instance["instance_summary"][function_instance]["invocation_summary"][
                            successor_function_str
                        ] = {
                            "invocation_count": 0,
                            "average_data_transfer_size": 0,
                            "transmission_summary": {},
                        }
                    workflow_summary_instance["instance_summary"][function_instance]["invocation_summary"][
                        successor_function_str
                    ]["invocation_count"] += 1
                    if successor_function_str not in data_transfer_sizes:
                        data_transfer_sizes[successor_function_str] = []
                    data_transfer_size = re.search(r"PAYLOAD_SIZE \((.*?)\)", log_entry)
                    if data_transfer_size:
                        data_transfer_sizes[successor_function_str].append(float(data_transfer_size.group(1)))

        for successor_function_str, data_transfer_size_list in data_transfer_sizes.items():
            average_data_transfer_size = (
                sum(data_transfer_size_list) / len(data_transfer_size_list) if data_transfer_size_list else 0
            )
            workflow_summary_instance["instance_summary"][function_instance]["invocation_summary"][
                successor_function_str
            ]["average_data_transfer_size"] = average_data_transfer_size

        workflow_summary_instance["instance_summary"][function_instance]["invocation_count"] += invocation_count
        workflow_summary_instance["instance_summary"][function_instance]["execution_summary"][
            f"{provider_region['provider']}:{provider_region['region']}"
        ]["invocation_count"] += invocation_count

        average_runtime = sum(runtimes) / len(runtimes) if runtimes else 0
        tail_runtime = max(runtimes) if runtimes else 0

        # convert to seconds
        average_runtime = average_runtime / 1000
        tail_runtime = tail_runtime / 1000

        workflow_summary_instance["instance_summary"][function_instance]["execution_summary"][
            f"{provider_region['provider']}:{provider_region['region']}"
        ]["average_runtime"] = average_runtime
        workflow_summary_instance["instance_summary"][function_instance]["execution_summary"][
            f"{provider_region['provider']}:{provider_region['region']}"
        ]["tail_runtime"] = tail_runtime

        return entry_point_invocation_count
