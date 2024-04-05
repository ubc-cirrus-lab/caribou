import json
from typing import Any

from multi_x_serverless.common.constants import WORKFLOW_SUMMARY_TABLE
from multi_x_serverless.common.models.remote_client.remote_client import RemoteClient
from multi_x_serverless.data_collector.components.data_retriever import DataRetriever


class WorkflowRetriever(DataRetriever):
    def __init__(self, client: RemoteClient) -> None:
        super().__init__(client)
        self._workflow_summary_table: str = WORKFLOW_SUMMARY_TABLE

    def retrieve_all_workflow_ids(self) -> set[str]:
        # Perhaps there could be a get all keys method in the remote client
        return set(self._client.get_keys(self._workflow_summary_table))

    def retrieve_workflow_summary(self, workflow_unique_id: str) -> dict[str, Any]:
        # Load the summarized logs from the workflow summary table
        workflow_summarized: str = self._client.get_value_from_table(self._workflow_summary_table, workflow_unique_id)

        # Consolidate all the timestamps together to one summary and return the result
        return self._transform_workflow_summary(workflow_summarized)

    def _transform_workflow_summary(self, workflow_summarized: str) -> dict[str, Any]:
        summarized_workflow = json.loads(workflow_summarized)

        start_hop_summary, instance_summary = self._construct_summaries(summarized_workflow.get("logs", {}))

        return {
            "workflow_runtime_samples": summarized_workflow["workflow_runtime_samples"],
            "daily_invocation_counts": summarized_workflow.get("daily_invocation_counts", {}),
            "start_hop_summary": start_hop_summary,
            "instance_summary": instance_summary,
        }

    def _construct_summaries(self, logs: list[dict[str, Any]]) -> tuple[dict[str, Any], dict[str, Any]]:
        start_hop_summary: dict[str, Any] = {}
        instance_summary: dict[str, Any] = {}

        for log in logs:
            self._extend_start_hop_summary(start_hop_summary, log)
            self._extend_instance_summary(instance_summary, log)

        return start_hop_summary, instance_summary

    def _extend_start_hop_summary(self, start_hop_summary: dict[str, Any], log: dict[str, Any]) -> None:
        start_hop_destination = log.get("start_hop_destination", None)
        if start_hop_destination:
            start_hop_destination_str = f'{start_hop_destination["provider"]}:{start_hop_destination["region"]}'
            if start_hop_destination_str not in start_hop_summary:
                start_hop_summary[start_hop_destination_str] = {}
            start_hop_data_transfer_size = float(log.get("start_hop_data_transfer_size", 0))
            if start_hop_data_transfer_size not in start_hop_summary[start_hop_destination_str]:
                start_hop_summary[start_hop_destination_str][start_hop_data_transfer_size] = []
            if log.get("start_hop_latency", None) is not None:
                start_hop_summary[start_hop_destination_str][start_hop_data_transfer_size].append(
                    log.get("start_hop_latency")
                )

    def _extend_instance_summary(  # pylint: disable=too-many-branches
        self, instance_summary: dict[str, Any], log: dict[str, Any]
    ) -> None:
        self._handle_execution_latencies(log, instance_summary)

        self._handle_region_to_region_transmission(log, instance_summary)

        self._handle_missing_region_to_region_transmission_data(instance_summary)

        self._handle_non_executions(log, instance_summary)

        self._calculate_invocation_probability(instance_summary)

    def _handle_execution_latencies(self, log: dict[str, Any], instance_summary: dict[str, Any]) -> None:
        for instance, execution_latency_information in log["execution_latencies"].items():
            if instance not in instance_summary:
                instance_summary[instance] = {"invocations": 0, "executions": {}, "to_instance": {}}
            instance_summary[instance]["invocations"] += 1
            provider_region_str = execution_latency_information["provider_region"]
            if provider_region_str not in instance_summary[instance]["executions"]:
                instance_summary[instance]["executions"][provider_region_str] = []
            instance_summary[instance]["executions"][provider_region_str].append(
                execution_latency_information["latency"]
            )

    def _handle_region_to_region_transmission(self, log: dict[str, Any], instance_summary: dict[str, Any]) -> None:
        for data in log["transmission_data"]:
            from_instance = data["from_instance"]
            to_instance = data["to_instance"]
            if from_instance not in instance_summary:
                instance_summary[from_instance] = {"invocations": 0, "executions": {}, "to_instance": {}}
            if to_instance not in instance_summary[from_instance]["to_instance"]:
                instance_summary[from_instance]["to_instance"][to_instance] = {
                    "invoked": 0,
                    "regions_to_regions": {},
                    "non_executions": 0,
                }
            instance_summary[from_instance]["to_instance"][to_instance]["invoked"] += 1

            from_region_str = data["from_region"]["provider"] + ":" + data["from_region"]["region"]
            to_region_str = data["to_region"]["provider"] + ":" + data["to_region"]["region"]
            if from_region_str not in instance_summary[from_instance]["to_instance"][to_instance]["regions_to_regions"]:
                instance_summary[from_instance]["to_instance"][to_instance]["regions_to_regions"][from_region_str] = {}
            if (
                to_region_str
                not in instance_summary[from_instance]["to_instance"][to_instance]["regions_to_regions"][
                    from_region_str
                ]
            ):
                instance_summary[from_instance]["to_instance"][to_instance]["regions_to_regions"][from_region_str][
                    to_region_str
                ] = {
                    "transfer_size_to_transfer_latencies": {},
                    "transfer_sizes": [],
                }

            transmission_data_transfer_size = data["transmission_size"]
            instance_summary[from_instance]["to_instance"][to_instance]["regions_to_regions"][from_region_str][
                to_region_str
            ]["transfer_sizes"].append(transmission_data_transfer_size)

            transmission_data_transfer_size_str = str(transmission_data_transfer_size)

            if (
                transmission_data_transfer_size_str
                not in instance_summary[from_instance]["to_instance"][to_instance]["regions_to_regions"][
                    from_region_str
                ][to_region_str]["transfer_size_to_transfer_latencies"]
            ):
                instance_summary[from_instance]["to_instance"][to_instance]["regions_to_regions"][from_region_str][
                    to_region_str
                ]["transfer_size_to_transfer_latencies"][transmission_data_transfer_size_str] = []
            instance_summary[from_instance]["to_instance"][to_instance]["regions_to_regions"][from_region_str][
                to_region_str
            ]["transfer_size_to_transfer_latencies"][transmission_data_transfer_size_str].append(
                data["transmission_latency"]
            )

    def _handle_missing_region_to_region_transmission_data(self, instance_summary: dict[str, Any]) -> None:
        for instance_val in instance_summary.values():  # pylint: disable=too-many-nested-blocks
            to_instances = instance_val.get("to_instance", {})
            for to_instance_val in to_instances.values():
                regions_to_regions = to_instance_val.get("regions_to_regions", {})

                for to_regions in regions_to_regions.values():
                    all_transfer_sizes = set()
                    # Aggregate all sizes and latencies
                    for transfer_information in to_regions.values():
                        all_transfer_sizes.update(
                            [float(data) for data in transfer_information["transfer_size_to_transfer_latencies"].keys()]
                        )

                    # Calculate global averages for each size
                    global_avg_latency_per_size = {}
                    for size in all_transfer_sizes:
                        total_latencies = []
                        for transfer_information in to_regions.values():
                            latencies = transfer_information["transfer_size_to_transfer_latencies"].get(size)
                            if latencies:
                                total_latencies.extend(latencies)
                        if total_latencies:
                            global_avg_latency_per_size[size] = sum(total_latencies) / len(total_latencies)

                    # Scale missing latencies based on the common transfer size (if available)
                    for transfer_information in to_regions.values():
                        existing_sizes = {
                            float(data) for data in transfer_information["transfer_size_to_transfer_latencies"].keys()
                        }
                        missing_sizes = all_transfer_sizes - existing_sizes

                        for missing_size in missing_sizes:
                            # Find the nearest size for which we have data
                            try:
                                nearest_size = min(
                                    transfer_information["transfer_size_to_transfer_latencies"].keys(),
                                    key=lambda x, missing_size=missing_size: abs(float(x) - float(missing_size)),  # type: ignore  # pylint: disable=line-too-long
                                )
                                scaling_factor = (
                                    global_avg_latency_per_size[missing_size] / global_avg_latency_per_size[nearest_size]
                                    if nearest_size in global_avg_latency_per_size
                                    else 1
                                )

                                scaled_latencies = [
                                    latency * scaling_factor
                                    for latency in transfer_information["transfer_size_to_transfer_latencies"][nearest_size]
                                ]
                                transfer_information["transfer_size_to_transfer_latencies"][missing_size] = scaled_latencies
                                transfer_information["transfer_sizes"].extend([missing_size] * len(scaled_latencies))
                            except KeyError:
                                pass

    def _handle_non_executions(self, log: dict[str, Any], instance_summary: dict[str, Any]) -> None:
        non_executions = log.get("non_executions", {})
        for caller, non_execution in non_executions.items():
            if caller not in instance_summary:
                instance_summary[caller] = {"invocations": 0, "executions": {}, "to_instance": {}}
            for callee, count in non_execution.items():
                if callee not in instance_summary[caller]["to_instance"]:
                    instance_summary[caller]["to_instance"][callee] = {
                        "invoked": 0,
                        "regions_to_regions": {},
                        "non_executions": 0,
                    }

                instance_summary[caller]["to_instance"][callee]["non_executions"] += count

    def _calculate_invocation_probability(self, instance_summary: dict[str, Any]) -> None:
        for to_instance in instance_summary.values():
            for caller_callee_data in to_instance["to_instance"].values():
                caller_callee_data["invocation_probability"] = caller_callee_data["invoked"] / (
                    caller_callee_data["invoked"] + caller_callee_data["non_executions"]
                )
