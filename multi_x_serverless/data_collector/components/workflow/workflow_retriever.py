import json
from typing import Any

from multi_x_serverless.common.constants import WORKFLOW_SUMMARY_TABLE
from multi_x_serverless.data_collector.components.data_retriever import DataRetriever
from multi_x_serverless.deployment.common.remote_client.remote_client import RemoteClient


class WorkflowRetriever(DataRetriever):
    def __init__(self, client: RemoteClient) -> None:
        super().__init__(client)
        self._workflow_summary_table: str = WORKFLOW_SUMMARY_TABLE

    def retrieve_all_workflow_ids(self) -> set[str]:
        # Perhaps there could be a get all keys method in the remote client
        return set(self._client.get_all_values_from_table(self._workflow_summary_table).keys())

    def retrieve_workflow_summary(self, workflow_unique_id: str) -> dict[str, Any]:
        # Load the summarized logs from the workflow summary table
        workflow_summarized_logs: dict[str, Any] = self._client.get_values_from_complex_key_table(
            self._workflow_summary_table, workflow_unique_id
        )

        # Consolidate all the timestamps together to one summary and return the result
        return self._consolidate_logs(workflow_summarized_logs)

    def _consolidate_logs(self, logs: dict[str, Any]) -> dict[str, Any]:
        # Here are the list of all keys in the available regions
        available_regions_set: set[str] = set(self._available_regions.keys())

        consolidated: dict[str, Any] = {}
        total_months = 0
        for _, data in logs.items():
            total_months += data["months_between_summary"]
            for instance_id, instance_data in data["instance_summary"].items():
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
                            "total_runtime": 0,
                            "total_tail_runtime": 0,
                        }
                    consolidated[instance_id]["execution_summary"][region]["invocation_count"] += region_data[
                        "invocation_count"
                    ]
                    consolidated[instance_id]["execution_summary"][region]["total_runtime"] += (
                        region_data["average_runtime"] * region_data["invocation_count"]
                    )
                    consolidated[instance_id]["execution_summary"][region]["total_tail_runtime"] += (
                        region_data["tail_runtime"] * region_data["invocation_count"]
                    )

                if "invocation_summary" in instance_data:
                    for child_instance, invocation_data in instance_data["invocation_summary"].items():
                        if child_instance not in consolidated[instance_id]["invocation_summary"]:
                            consolidated[instance_id]["invocation_summary"][child_instance] = {
                                "invocation_count": 0,
                                "total_data_transfer_size": 0,
                                "transmission_summary": {},
                            }
                        consolidated[instance_id]["invocation_summary"][child_instance][
                            "invocation_count"
                        ] += invocation_data["invocation_count"]
                        consolidated[instance_id]["invocation_summary"][child_instance]["total_data_transfer_size"] += (
                            invocation_data["average_data_transfer_size"] * invocation_data["invocation_count"]
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
                                                    "total_latency": 0,
                                                    "total_tail_latency": 0,
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
                                            ][from_provider_region][to_provider_region]["total_latency"] += (
                                                to_provider_transmission_data["average_latency"]
                                                * to_provider_transmission_data["transmission_count"]
                                            )
                                            consolidated[instance_id]["invocation_summary"][child_instance][
                                                "transmission_summary"
                                            ][from_provider_region][to_provider_region]["total_tail_latency"] += (
                                                to_provider_transmission_data["tail_latency"]
                                                * to_provider_transmission_data["transmission_count"]
                                            )

        # Summarized data in proper output format
        workflow_summary_data: dict[str, Any] = {}
        for instance_id, instance_data in consolidated.items():
            # Home region average/tail runtime
            # Only regions within the available regions list is allowed
            filtered_execution_summary = {
                region: data
                for region, data in instance_data["execution_summary"].items()
                if region in available_regions_set
            }
            favourite_home_region = max(
                filtered_execution_summary, key=lambda region: filtered_execution_summary[region]["invocation_count"]
            )

            # Now for execution summary only in the available regions
            execution_summary: dict[str, Any] = {}
            for region, region_data in filtered_execution_summary.items():
                execution_summary[region] = {
                    "average_runtime": region_data["total_runtime"] / region_data["invocation_count"],
                    "tail_runtime": region_data["total_tail_runtime"] / region_data["invocation_count"],
                }

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
                                            "average_latency": to_provider_transmission_data["total_latency"]
                                            / to_provider_transmission_data["transmission_count"],
                                            "tail_latency": to_provider_transmission_data["total_tail_latency"]
                                            / to_provider_transmission_data["transmission_count"],
                                        }

                    # Manage invocation summary
                    invocation_summary["probability_of_invocation"] = (
                        invocation_data["invocation_count"] / instance_data["invocation_count"]
                    )
                    invocation_summary["average_data_transfer_size"] = (
                        invocation_data["total_data_transfer_size"] / invocation_data["invocation_count"]
                    )
                    invocation_summary["transmission_summary"] = transmission_summary

            # Final output
            workflow_summary_data[instance_id] = {
                "favourite_home_region": favourite_home_region,
                "favourite_home_region_average_runtime": filtered_execution_summary[favourite_home_region][
                    "total_runtime"
                ]
                / filtered_execution_summary[favourite_home_region]["invocation_count"],
                "favourite_home_region_tail_runtime": filtered_execution_summary[favourite_home_region][
                    "total_tail_runtime"
                ]
                / filtered_execution_summary[favourite_home_region]["invocation_count"],
                "projected_monthly_invocations": instance_data["invocation_count"]
                / total_months,  # Simple Estimation, may not be accurate
                "execution_summary": execution_summary,
                "invocation_summary": invocation_summary,
            }

        return workflow_summary_data
