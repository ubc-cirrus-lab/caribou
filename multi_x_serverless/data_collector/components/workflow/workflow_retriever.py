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
        # Handle execution latencies
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

        # Handle regions to regions transmission
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
                ] = {}

            transmission_data_transfer_size = data["transmission_size"]
            if (
                transmission_data_transfer_size
                not in instance_summary[from_instance]["to_instance"][to_instance]["regions_to_regions"][
                    from_region_str
                ][to_region_str]
            ):
                instance_summary[from_instance]["to_instance"][to_instance]["regions_to_regions"][from_region_str][
                    to_region_str
                ][transmission_data_transfer_size] = []
            instance_summary[from_instance]["to_instance"][to_instance]["regions_to_regions"][from_region_str][
                to_region_str
            ][transmission_data_transfer_size].append(data["transmission_latency"])

        # Handle non-executions
        non_executions = log.get("non_executions", {})
        for caller, non_execution in non_executions.items():
            if caller not in instance_summary:
                instance_summary[caller] = {"invocations": 0, "executions": {}, "to_instance": {}}
            for callee, count in non_execution.items():
                if callee not in instance_summary[caller]:
                    instance_summary[caller]["to_instance"][callee] = {
                        "invoked": 0,
                        "regions_to_regions": {},
                        "non_executions": 0,
                    }

                instance_summary[caller]["to_instance"][callee]["non_executions"] += count

        # Calculate the invocation probability
        for caller, to_instance in instance_summary.items():
            for callee, caller_callee_data in to_instance["to_instance"].items():
                caller_callee_data["invocation_probability"] = caller_callee_data["invoked"] / (
                    caller_callee_data["invoked"] + caller_callee_data["non_executions"]
                )
