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
        return set(self._client.get_all_values_from_table(self._workflow_summary_table).keys())

    def retrieve_workflow_summary(self, workflow_unique_id: str) -> dict[str, Any]:
        # Load the summarized logs from the workflow summary table
        workflow_summarized_logs: list[str] = self._client.get_all_values_from_sort_key_table(
            self._workflow_summary_table, workflow_unique_id
        )

        # Consolidate all the timestamps together to one summary and return the result
        return self._consolidate_logs(workflow_summarized_logs)

    def _consolidate_logs(self, logs: list[str]) -> dict[str, Any]:  # pylint: disable=too-many-branches
        # Here are the list of all keys in the available regions
        available_regions_set: set[str] = set(self._available_regions.keys())

        consolidated: dict[str, Any] = {}
        total_invocations = 0
        total_days = 0
        for data in logs:  # pylint: disable=too-many-nested-blocks
            loaded_data = json.loads(data)
            total_days += loaded_data["time_since_last_sync"]
            total_invocations += loaded_data["total_invocations"]
            for instance_id, instance_data in loaded_data["instance_summary"].items():
                if instance_id not in consolidated:
                    consolidated[instance_id] = {
                        "invocation_count": 0,
                        "execution_summary": {},
                        "invocation_summary": {},
                    }
                consolidated[instance_id]["invocation_count"] += instance_data["invocation_count"]

                # Deal with execution summary
                for region, region_data in instance_data["execution_summary"].items():
                    if region not in consolidated[instance_id]["execution_summary"]:
                        consolidated[instance_id]["execution_summary"][region] = {
                            "invocation_count": 0,
                            "runtime_samples": [],
                        }

                    # Check if this node is an entry point and if yes, add the data transfer and latency samples
                    if "init_data_transfer_size_samples" in region_data:
                        if (
                            "init_data_transfer_size_samples"
                            not in consolidated[instance_id]["execution_summary"][region]
                        ):
                            consolidated[instance_id]["execution_summary"][region][
                                "init_data_transfer_size_samples"
                            ] = []
                        consolidated[instance_id]["execution_summary"][region][
                            "init_data_transfer_size_samples"
                        ].extend(region_data["init_data_transfer_size_samples"])

                    if "init_latency_samples" in region_data:
                        if "init_latency_samples" not in consolidated[instance_id]["execution_summary"][region]:
                            consolidated[instance_id]["execution_summary"][region]["init_latency_samples"] = []
                        consolidated[instance_id]["execution_summary"][region]["init_latency_samples"].extend(
                            [sample / 1000 for sample in region_data["init_latency_samples"]]  # Convert to seconds
                        )

                    # Add the runtime samples
                    consolidated[instance_id]["execution_summary"][region]["invocation_count"] += region_data[
                        "invocation_count"
                    ]
                    consolidated[instance_id]["execution_summary"][region]["runtime_samples"].extend(
                        [sample / 1000 for sample in region_data["runtime_samples"]]  # Convert to seconds
                    )

                if "invocation_summary" in instance_data:
                    for child_instance, invocation_data in instance_data["invocation_summary"].items():
                        if child_instance not in consolidated[instance_id]["invocation_summary"]:
                            consolidated[instance_id]["invocation_summary"][child_instance] = {
                                "invocation_count": 0,
                                "data_transfer_samples": [],
                                "transmission_summary": {},
                            }
                        consolidated[instance_id]["invocation_summary"][child_instance][
                            "invocation_count"
                        ] += invocation_data["invocation_count"]
                        consolidated[instance_id]["invocation_summary"][child_instance]["data_transfer_samples"].extend(
                            invocation_data["data_transfer_samples"]
                        )

                        # Deal with transmission summary
                        if "transmission_summary" in invocation_data:
                            for from_provider_region, from_provider_transmission_data in invocation_data[
                                "transmission_summary"
                            ].items():
                                if from_provider_region in available_regions_set:
                                    if (
                                        from_provider_region
                                        not in consolidated[instance_id]["invocation_summary"][child_instance][
                                            "transmission_summary"
                                        ]
                                    ):
                                        consolidated[instance_id]["invocation_summary"][child_instance][
                                            "transmission_summary"
                                        ][from_provider_region] = {}

                                    for (
                                        to_provider_region,
                                        to_provider_transmission_data,
                                    ) in from_provider_transmission_data.items():
                                        if to_provider_region in available_regions_set:
                                            if (
                                                to_provider_region
                                                not in consolidated[instance_id]["invocation_summary"][child_instance][
                                                    "transmission_summary"
                                                ][from_provider_region]
                                            ):
                                                consolidated[instance_id]["invocation_summary"][child_instance][
                                                    "transmission_summary"
                                                ][from_provider_region][to_provider_region] = {
                                                    "transmission_count": 0,
                                                    "latency_samples": [],
                                                }

                                            consolidated[instance_id]["invocation_summary"][child_instance][
                                                "transmission_summary"
                                            ][from_provider_region][to_provider_region][
                                                "transmission_count"
                                            ] += to_provider_transmission_data[
                                                "transmission_count"
                                            ]
                                            consolidated[instance_id]["invocation_summary"][child_instance][
                                                "transmission_summary"
                                            ][from_provider_region][to_provider_region]["latency_samples"].extend(
                                                [
                                                    sample / 1000
                                                    for sample in to_provider_transmission_data["latency_samples"]
                                                ]
                                            )

        # Summarized data in proper output format
        workflow_summary_data: dict[str, Any] = {}
        workflow_summary_data["total_invocations"] = total_invocations
        for instance_id, instance_data in consolidated.items():  # pylint: disable=too-many-nested-blocks
            # Home region average/tail runtime
            # Only regions within the available regions list is allowed
            filtered_execution_summary = {
                region: data
                for region, data in instance_data["execution_summary"].items()
                if region in available_regions_set
            }

            # Now for execution summary only in the available regions
            execution_summary: dict[str, Any] = {}
            for region, region_data in filtered_execution_summary.items():
                execution_summary[region] = {
                    "runtime_samples": region_data["runtime_samples"],
                    "invocation_count": region_data["invocation_count"],
                    "unit": "s",
                }

                if "init_data_transfer_size_samples" in region_data:
                    execution_summary[region]["init_data_transfer_size_samples"] = region_data[
                        "init_data_transfer_size_samples"
                    ]
                if "init_latency_samples" in region_data:
                    execution_summary[region]["init_latency_samples"] = region_data["init_latency_samples"]

            # Now for invocation summary
            # Region restrictions were already applied
            invocation_summary: dict[str, Any] = {}
            if "invocation_summary" in instance_data:
                for child_instance, invocation_data in instance_data["invocation_summary"].items():
                    # Manage Tranmission summary
                    transmission_summary: dict[str, Any] = {}
                    if "transmission_summary" in invocation_data:
                        for from_provider_region, from_provider_data in invocation_data["transmission_summary"].items():
                            if from_provider_region in available_regions_set:
                                transmission_summary[from_provider_region] = {}
                                for to_provider_region, to_provider_transmission_data in from_provider_data.items():
                                    if to_provider_region in available_regions_set:
                                        transmission_summary[from_provider_region][to_provider_region] = {
                                            "latency_samples": to_provider_transmission_data["latency_samples"],
                                            "unit": "s",
                                        }

                    # Manage invocation summary
                    invocation_summary[child_instance] = {
                        "probability_of_invocation": invocation_data["invocation_count"]
                        / instance_data["invocation_count"],
                        "data_transfer_samples": invocation_data["data_transfer_samples"],
                        "transmission_summary": transmission_summary,
                    }

            # Final output
            total_months = total_days / 30
            workflow_summary_data[instance_id] = {
                "projected_monthly_invocations": instance_data["invocation_count"]
                / total_months,  # Simple Estimation, may not be accurate
                "execution_summary": execution_summary,
                "invocation_summary": invocation_summary,
            }

        return workflow_summary_data
