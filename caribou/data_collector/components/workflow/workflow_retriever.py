import json
import math
from typing import Any, Optional

import numpy as np
from scipy import stats

from caribou.common.constants import CONDITIONALLY_NOT_INVOKE_TASK_TYPE, WORKFLOW_SUMMARY_TABLE
from caribou.common.models.remote_client.remote_client import RemoteClient
from caribou.data_collector.components.data_retriever import DataRetriever


class WorkflowRetriever(DataRetriever):
    def __init__(self, client: RemoteClient) -> None:
        super().__init__(client)
        self._workflow_summary_table: str = WORKFLOW_SUMMARY_TABLE

    def retrieve_all_workflow_ids(self) -> set[str]:
        # Perhaps there could be a get all keys method in the remote client
        return set(self._client.get_keys(self._workflow_summary_table))

    def retrieve_workflow_summary(self, workflow_unique_id: str) -> dict[str, Any]:
        # Load the summarized logs from the workflow summary table
        workflow_summarized, _ = self._client.get_value_from_table(self._workflow_summary_table, workflow_unique_id)

        # Consolidate all the timestamps together to one summary and return the result
        return self._transform_workflow_summary(workflow_summarized)

    def _transform_workflow_summary(self, workflow_summarized: str) -> dict[str, Any]:
        if workflow_summarized == "":
            return {}
        summarized_workflow = json.loads(workflow_summarized)

        start_hop_summary, instance_summary, runtime_samples = self._construct_summaries(
            summarized_workflow.get("logs", {})
        )

        return {
            "workflow_runtime_samples": runtime_samples,
            "daily_invocation_counts": summarized_workflow.get("daily_invocation_counts", {}),
            "daily_user_code_failure_counts": summarized_workflow.get("daily_user_code_failure_counts", {}),
            "start_hop_summary": start_hop_summary,
            "instance_summary": instance_summary,
        }

    def _construct_summaries(self, logs: list[dict[str, Any]]) -> tuple[dict[str, Any], dict[str, Any], list[float]]:
        start_hop_summary: dict[str, Any] = {
            "invoked": 0,
            "retrieved_wpd_at_function": 0,
            "wpd_at_function_probability": 0.0,
            "workflow_placement_decision_size_gb": [],
            "at_redirector": {},
            "from_client": {
                "transfer_sizes_gb": [],
                "received_region": {},
            },
        }
        instance_summary: dict[str, Any] = {}
        runtime_samples: list[float] = []

        for log in logs:
            # Add to sample runtime
            workflow_runtime = log.get("runtime_s", None)
            if workflow_runtime is not None:
                runtime_samples.append(workflow_runtime)

            # Add to start hop summary
            self._extend_start_hop_summary(start_hop_summary, log)

            # Add to instance summary
            self._extend_instance_summary(instance_summary, log)

        # Perform post processing on start hop summary
        self._reorganize_start_hop_summary(start_hop_summary)

        # Perform post processing on instance summary
        self._reorganize_instance_summary(instance_summary)

        return start_hop_summary, instance_summary, runtime_samples

    def _extend_start_hop_summary(self, start_hop_summary: dict[str, Any], log: dict[str, Any]) -> None:
        from_client = start_hop_summary["from_client"]
        start_hop_size_latency_summary = from_client["received_region"]

        start_hop_log: dict[str, Any] = log.get("start_hop_info", None)
        if start_hop_log:
            # Determine if the workflow placement decision was retrieved at the function
            # Redirect only occurs if the workflow placement decision was retrieved at the function
            # Otherwise it would be directly send to appropriate region
            start_hop_summary["invoked"] += 1
            has_retrieved_wpd_at_function = start_hop_log.get("workflow_placement_decision", {}).get(
                "retrieved_wpd_at_function", False
            )
            if has_retrieved_wpd_at_function:
                start_hop_summary["retrieved_wpd_at_function"] += 1

            # Determine the start hop latency from the client
            start_hop_destination = start_hop_log.get("destination", None)
            if start_hop_destination:
                if start_hop_destination not in start_hop_size_latency_summary:
                    start_hop_size_latency_summary[start_hop_destination] = {
                        "transfer_size_gb_to_transfer_latencies_s": {},
                    }

                start_hop_data_transfer_size = float(start_hop_log.get("data_transfer_size_gb", 0.0))

                # # Round start hop data transfer size to nearest 1 KB
                # start_hop_data_transfer_size = self._round_to_kb(start_hop_data_transfer_size, 1)

                # Add the transfer size to the summary
                from_client["transfer_sizes_gb"].append(start_hop_data_transfer_size)

                # Round start hop data transfer size to nearest 10 KB
                start_hop_data_transfer_size = self._round_to_kb(start_hop_data_transfer_size, 10)
                if (
                    start_hop_data_transfer_size
                    not in start_hop_size_latency_summary[start_hop_destination][
                        "transfer_size_gb_to_transfer_latencies_s"
                    ]
                ):
                    start_hop_size_latency_summary[start_hop_destination]["transfer_size_gb_to_transfer_latencies_s"][
                        start_hop_data_transfer_size
                    ] = []
                start_hop_latency = start_hop_log.get("latency_from_client_s", 0.0)

                # If start hop is greater than 3 seconds, its likely that
                # the user clock is desynced and we may discard the data
                if 0 < start_hop_latency < 3.0:
                    start_hop_size_latency_summary[start_hop_destination]["transfer_size_gb_to_transfer_latencies_s"][
                        start_hop_data_transfer_size
                    ].append(start_hop_latency)

            # Add workflow_placement_decision size to the summary
            workflow_placement_decision_size = start_hop_log.get("workflow_placement_decision", {}).get(
                "data_size_gb", None
            )
            if workflow_placement_decision_size is not None:
                # # Round to nearest 1 KB
                # workflow_placement_decision_size = self._round_to_kb(workflow_placement_decision_size, 1)
                start_hop_summary["workflow_placement_decision_size_gb"].append(workflow_placement_decision_size)

            # Now also fill in at_redirector data
            redirector_execution_data: dict[str, Any] = start_hop_log.get("redirector_execution_data", None)
            if redirector_execution_data:
                at_redirector = start_hop_summary["at_redirector"]
                self._handle_single_execution_data_entry(redirector_execution_data, at_redirector)

    def _extend_instance_summary(  # pylint: disable=too-many-branches
        self, instance_summary: dict[str, Any], log: dict[str, Any]
    ) -> None:
        self._handle_execution_data(log, instance_summary)

        self._handle_region_to_region_transmission(log, instance_summary)

    def _handle_execution_data(self, log: dict[str, Any], instance_summary: dict[str, Any]) -> None:
        for execution_information in log["execution_data"]:
            # Handle the single execution data entry
            self._handle_single_execution_data_entry(execution_information, instance_summary)

    # pylint: disable=too-many-branches, too-many-nested-blocks
    def _handle_single_execution_data_entry(
        self, execution_information: dict[str, Any], instance_summary: dict[str, Any]
    ) -> None:
        instance = execution_information["instance_name"]
        provider_region = execution_information["provider_region"]

        # Create the missing dictionary entries
        if instance not in instance_summary:
            instance_summary[instance] = {}
        if "invocations" not in instance_summary[instance]:
            instance_summary[instance]["invocations"] = 0
        if "cpu_utilization" not in instance_summary[instance]:
            instance_summary[instance]["cpu_utilization"] = []
        if "executions" not in instance_summary[instance]:
            instance_summary[instance]["executions"] = {
                "at_region": {},
                "successor_instances": set(),
            }

        if provider_region not in instance_summary[instance]["executions"]["at_region"]:
            instance_summary[instance]["executions"]["at_region"][provider_region] = []

        # Append the number of invocations
        instance_summary[instance]["invocations"] += 1

        # Append an entry of the cpu utilization
        instance_summary[instance]["cpu_utilization"].append(execution_information["cpu_utilization"])

        # Process execution data
        execution_data = {
            "duration_s": execution_information["duration_s"],
            "cpu_utilization": execution_information["cpu_utilization"],
            "data_transfer_during_execution_gb": execution_information["data_transfer_during_execution_gb"],
            "successor_invocations": {},
        }

        successor_data: Optional[dict[str, Any]] = execution_information.get("successor_data", None)
        if successor_data is not None:
            for successor, successor_info in successor_data.items():
                invocation_time_from_function_start_s = successor_info["invocation_time_from_function_start_s"]

                # Round to nearest ms (As we are dealing with time)
                invocation_time_from_function_start_s = self._round_to_ms(invocation_time_from_function_start_s)

                execution_data["successor_invocations"][successor] = {
                    "invocation_time_from_function_start_s": invocation_time_from_function_start_s,
                }
                instance_summary[instance]["executions"]["successor_instances"].add(successor)

        instance_summary[instance]["executions"]["at_region"][provider_region].append(execution_data)

        # Deal with the successor non-execution data
        successor_data = execution_information.get("successor_data", None)
        if successor_data is not None:
            for successor, successor_info in successor_data.items():
                # Get the task type of the successor data
                task_type = successor_info.get("task_type", None)

                if task_type == CONDITIONALLY_NOT_INVOKE_TASK_TYPE:
                    # Create the missing dictionary entries
                    caller = instance
                    callee = successor
                    if caller not in instance_summary:
                        instance_summary[caller] = {}
                    if "to_instance" not in instance_summary[caller]:
                        instance_summary[caller]["to_instance"] = {}
                    if callee not in instance_summary[caller]["to_instance"]:
                        instance_summary[caller]["to_instance"][callee] = {
                            "invoked": 0,
                            "non_executions": 0,
                            "invocation_probability": 0.0,
                            "sync_size_gb": [],
                            "sns_only_size_gb": [],
                            "transfer_sizes_gb": [],
                            "regions_to_regions": {},
                            "non_execution_info": {},
                        }

                    # Mark this as a non-execution
                    instance_summary[caller]["to_instance"][callee]["non_executions"] += 1

                    # Add the sync info
                    sync_info = successor_info.get("sync_info", None)
                    if sync_info is not None:
                        for sync_to_from_instance, sync_instance_info in sync_info.items():
                            # Add dictionary entries
                            if (
                                sync_to_from_instance
                                not in instance_summary[caller]["to_instance"][callee]["non_execution_info"]
                            ):
                                instance_summary[caller]["to_instance"][callee]["non_execution_info"][
                                    sync_to_from_instance
                                ] = {
                                    "sync_data_response_size_gb": [],
                                    "sns_transfer_size_gb": [],
                                    "regions_to_regions": {},
                                }

                            # Get the consumed write capacity and sync data response size
                            sync_data_response_size = sync_instance_info.get("sync_data_response_size_gb", None)
                            if sync_data_response_size is not None:
                                instance_summary[caller]["to_instance"][callee]["non_execution_info"][
                                    sync_to_from_instance
                                ]["sync_data_response_size_gb"].append(sync_data_response_size)

    # pylint: disable=too-many-branches, too-many-statements
    def _handle_region_to_region_transmission(self, log: dict[str, Any], instance_summary: dict[str, Any]) -> None:
        for data in log["transmission_data"]:
            from_instance = data["from_instance"]
            uninvoked_instance = data.get("uninvoked_instance", None)
            to_instance = data["to_instance"]
            from_region = data["from_region"]
            to_region = data["to_region"]
            successor_invoked = data["successor_invoked"]
            from_direct_successor = data["from_direct_successor"]

            if from_direct_successor:
                # This is the case where the transmission is from a direct successor

                # Get the intended origin and destination instances
                # To create a common dictionary entry
                origin_instance = from_instance
                intended_destination_instance = to_instance
                if not from_direct_successor:
                    intended_destination_instance = uninvoked_instance

                # Create the missing dictionary entries (Common)
                if origin_instance not in instance_summary:
                    instance_summary[origin_instance] = {}
                if "to_instance" not in instance_summary[origin_instance]:
                    instance_summary[origin_instance]["to_instance"] = {}
                if intended_destination_instance not in instance_summary[origin_instance]["to_instance"]:
                    instance_summary[origin_instance]["to_instance"][intended_destination_instance] = {
                        "invoked": 0,
                        "non_executions": 0,
                        "invocation_probability": 0.0,
                        "sync_size_gb": [],
                        "sns_only_size_gb": [],
                        "transfer_sizes_gb": [],
                        "regions_to_regions": {},
                        "non_execution_info": {},
                    }

                # Increment invoked count (Even if not directly invoked)
                instance_summary[from_instance]["to_instance"][to_instance]["invoked"] += 1

                # Handle the transmission data
                ## First create the missing dictionary entries
                if from_region not in instance_summary[from_instance]["to_instance"][to_instance]["regions_to_regions"]:
                    instance_summary[from_instance]["to_instance"][to_instance]["regions_to_regions"][from_region] = {}
                if (
                    to_region
                    not in instance_summary[from_instance]["to_instance"][to_instance]["regions_to_regions"][
                        from_region
                    ]
                ):
                    instance_summary[from_instance]["to_instance"][to_instance]["regions_to_regions"][from_region][
                        to_region
                    ] = {
                        "transfer_size_gb_to_transfer_latencies_s": {},
                        "best_fit_line": {},
                    }

                # Add an entry for the transfer size
                # Right now we are making a simple assumption that
                # since wrapper should be small, the data transfer is
                # always the sum of the wrapper and potential data upload size.
                transmission_data_transfer_size = float(data["transmission_size_gb"])

                # Check if the transmission data also contain sync_information
                # Denoting if it uploads or recieves data from synchronization
                sync_data_upload_size = data.get("sync_information", {}).get("upload_size_gb", None)
                sync_information_sync_size = data.get("sync_information", {}).get("sync_data_response_size_gb", None)
                if sync_data_upload_size is not None:
                    instance_summary[from_instance]["to_instance"][to_instance]["sns_only_size_gb"].append(
                        transmission_data_transfer_size
                    )

                    # In the case of sync upload, we want to set the data
                    # transfer to be related only to upload size, as
                    # the sns size should always be the same and should be small
                    transmission_data_transfer_size = sync_data_upload_size
                if sync_information_sync_size is not None:
                    instance_summary[from_instance]["to_instance"][to_instance]["sync_size_gb"].append(
                        sync_information_sync_size
                    )

                # # Round to nearest kb (as dynamodb write is charged in kb)
                # transmission_data_transfer_size = self._round_to_kb(transmission_data_transfer_size, 1)

                instance_summary[from_instance]["to_instance"][to_instance]["transfer_sizes_gb"].append(
                    transmission_data_transfer_size
                )

                # Add an entry for the transfer latency
                # (Only for cases where successor_invoked is True)
                # Else we do not have the transfer latency for this
                # node as it is not directly invoked
                if successor_invoked:
                    # Round to nearest 10 KB (as data transfer latency should not be too granular)
                    transmission_data_transfer_size_str = str(self._round_to_kb(transmission_data_transfer_size, 10))
                    if (
                        transmission_data_transfer_size_str
                        not in instance_summary[from_instance]["to_instance"][to_instance]["regions_to_regions"][
                            from_region
                        ][to_region]["transfer_size_gb_to_transfer_latencies_s"]
                    ):
                        instance_summary[from_instance]["to_instance"][to_instance]["regions_to_regions"][from_region][
                            to_region
                        ]["transfer_size_gb_to_transfer_latencies_s"][transmission_data_transfer_size_str] = []
                    instance_summary[from_instance]["to_instance"][to_instance]["regions_to_regions"][from_region][
                        to_region
                    ]["transfer_size_gb_to_transfer_latencies_s"][transmission_data_transfer_size_str].append(
                        data["transmission_latency_s"]
                    )
            else:
                # This is the case where the transmission is not from a direct successor
                # Aka via non-execution, a node calls some potentially far descendant
                simulated_sync_predecessor = data.get("simulated_sync_predecessor", None)
                sync_node_insance = to_instance
                sync_to_from_instance = f"{simulated_sync_predecessor}>{sync_node_insance}"
                # Add dictionary entries
                if (
                    sync_to_from_instance
                    not in instance_summary[from_instance]["to_instance"][uninvoked_instance]["non_execution_info"]
                ):
                    instance_summary[from_instance]["to_instance"][uninvoked_instance]["non_execution_info"][
                        sync_to_from_instance
                    ] = {
                        # "consumed_write_capacity": [],
                        "sync_data_response_size_gb": [],
                        "sns_transfer_size_gb": [],
                        "regions_to_regions": {},
                    }
                if (
                    from_region
                    not in instance_summary[from_instance]["to_instance"][uninvoked_instance]["non_execution_info"][
                        sync_to_from_instance
                    ]["regions_to_regions"]
                ):
                    instance_summary[from_instance]["to_instance"][uninvoked_instance]["non_execution_info"][
                        sync_to_from_instance
                    ]["regions_to_regions"][from_region] = {}
                if (
                    to_region
                    not in instance_summary[from_instance]["to_instance"][uninvoked_instance]["non_execution_info"][
                        sync_to_from_instance
                    ]["regions_to_regions"][from_region]
                ):
                    instance_summary[from_instance]["to_instance"][uninvoked_instance]["non_execution_info"][
                        sync_to_from_instance
                    ]["regions_to_regions"][from_region][to_region] = {
                        "transfer_latencies_s": [],
                    }

                # Add an entry for the sns transfer size
                transmission_data_transfer_size = float(data["transmission_size_gb"])
                instance_summary[from_instance]["to_instance"][uninvoked_instance]["non_execution_info"][
                    sync_to_from_instance
                ]["sns_transfer_size_gb"].append(transmission_data_transfer_size)

                # Add an entry for the transfer latency
                instance_summary[from_instance]["to_instance"][uninvoked_instance]["non_execution_info"][
                    sync_to_from_instance
                ]["regions_to_regions"][from_region][to_region]["transfer_latencies_s"].append(
                    data["transmission_latency_s"]
                )

    def _reorganize_start_hop_summary(self, start_hop_summary: dict[str, Any]) -> None:
        # Here we simply average the workflow_placement_decision_size_gb
        # And round up to the nearest 1 KB
        workflow_placement_decision_size_gb = start_hop_summary["workflow_placement_decision_size_gb"]
        if workflow_placement_decision_size_gb:
            start_hop_summary["workflow_placement_decision_size_gb"] = sum(workflow_placement_decision_size_gb) / len(
                workflow_placement_decision_size_gb
            )
            # start_hop_summary["workflow_placement_decision_size_gb"] = self._round_to_kb(
            #     start_hop_summary["workflow_placement_decision_size_gb"], 1
            # )

        from_client = start_hop_summary["from_client"]
        for at_region_info in from_client["received_region"].values():
            transfer_size_to_transfer_latencies = at_region_info["transfer_size_gb_to_transfer_latencies_s"]

            number_of_data_sizes = len(transfer_size_to_transfer_latencies)
            if number_of_data_sizes == 0:
                # Case where there are no data
                continue

            at_region_info["best_fit_line"] = self._calculate_best_fit_line(transfer_size_to_transfer_latencies)

        # Summarize the execution data (Duration, data transfer, etc.)
        self._summarize_execution_data(start_hop_summary["at_redirector"])

        # Summarize the wpd_at_function_probability
        if start_hop_summary["invoked"] != 0:
            start_hop_summary["wpd_at_function_probability"] = (
                start_hop_summary["retrieved_wpd_at_function"] / start_hop_summary["invoked"]
            )

    def _reorganize_instance_summary(self, instance_summary: dict[str, Any]) -> None:
        # Summarize the execution data (Duration, data transfer, etc.)
        self._summarize_execution_data(instance_summary)

        # Summarize the non-execution data,
        # i.e. calculate the invocation probability
        self._summarize_non_execution_data(instance_summary)

        # Calculate the average sync table size
        self._calculate_average_sync_table_and_sns_size(instance_summary)

        # Handle the missing region to region transmission data
        # This is done by calculating the best fit line for the data
        self._handle_missing_region_to_region_transmission_data(instance_summary)

    def _summarize_execution_data(self, instance_summary: dict[str, Any]) -> None:
        for instance_val in instance_summary.values():
            # Handle the cpu utilization
            cpu_utilization = instance_val.get("cpu_utilization", [])
            if cpu_utilization:
                # Get the average cpu utilization but ensure
                # that if value > 1, set to 1, and if value < 0, set to 0
                cpu_utilization = [min(1.0, max(0.0, cpu)) for cpu in cpu_utilization]
                instance_val["cpu_utilization"] = sum(cpu_utilization) / len(cpu_utilization)

            # Handle the execution data
            execution_data = instance_val.get("executions", {})
            if execution_data:
                # Get list of all successor instances
                successor_instances = list(instance_val["executions"]["successor_instances"])

                # Now remove the successor instances from the dictionary
                del instance_val["executions"]["successor_instances"]

                # Get translation of successor instances
                index_translation = {
                    "data_transfer_during_execution_gb": 0,
                }
                original_index_length = len(index_translation)
                for index, successor_instance in enumerate(successor_instances):
                    index_translation[successor_instance] = index + original_index_length

                # Add the index translation to the instance_val
                instance_val["executions"]["auxiliary_index_translation"] = index_translation

                # Now translate the at_region data to be in a list of the
                # form with values stored in the order of index_translation
                for region, execution_data in instance_val["executions"]["at_region"].items():
                    durations = []
                    cpu_utilizations = []
                    duration_to_auxiliary_data: dict[float, Any] = {}

                    for execution in execution_data:
                        duration = execution["duration_s"]
                        durations.append(duration)

                        utilization = execution["cpu_utilization"]
                        cpu_utilizations.append(utilization)

                        # Round the duration to the nearest 10 ms
                        duration = self._round_to_ms(duration, 10)

                        # Make new auxiliary data if not present
                        if duration not in duration_to_auxiliary_data:
                            duration_to_auxiliary_data[duration] = []

                        new_execution = [None] * len(index_translation)
                        new_execution[0] = execution["data_transfer_during_execution_gb"]
                        for successor, successor_data in execution["successor_invocations"].items():
                            new_execution[index_translation[successor]] = successor_data[
                                "invocation_time_from_function_start_s"
                            ]

                        duration_to_auxiliary_data[duration].append(new_execution)

                    cpu_utilizations = [min(1.0, max(0.0, cpu)) for cpu in cpu_utilizations]
                    average_cpu_utilization = sum(cpu_utilizations) / len(cpu_utilizations)
                    instance_val["executions"]["at_region"][region] = {
                        "cpu_utilization": average_cpu_utilization,
                        "durations_s": durations,
                        "auxiliary_data": duration_to_auxiliary_data,
                    }

    def _summarize_non_execution_data(self, instance_summary: dict[str, Any]) -> None:
        for from_instance in instance_summary.values():
            for caller_callee_data in from_instance.get("to_instance", {}).values():
                # Calculate the invocation probability
                caller_callee_data["invocation_probability"] = caller_callee_data["invoked"] / (
                    caller_callee_data["invoked"] + caller_callee_data["non_executions"]
                )

                # Average the consumed write capacity and sync data response size
                non_execution_info = caller_callee_data.get("non_execution_info", None)
                if non_execution_info is not None:
                    # Summarize the consumed write capacity and sync data response size
                    for sync_call_from_to_instance, sync_call_entry in non_execution_info.items():
                        sync_data_response_size = sync_call_entry.get("sync_data_response_size_gb", [])
                        if sync_data_response_size != []:
                            sync_data_response_size_gb = sum(sync_data_response_size) / len(sync_data_response_size)

                            # # Round to the nearest kb
                            # sync_data_response_size_gb = self._round_to_kb(sync_data_response_size_gb, 1, False)

                            non_execution_info[sync_call_from_to_instance][
                                "sync_data_response_size_gb"
                            ] = sync_data_response_size_gb
                        else:
                            non_execution_info[sync_call_from_to_instance]["sync_data_response_size_gb"] = 0.0

                        sns_transfer_size = sync_call_entry.get("sns_transfer_size_gb", [])
                        if sns_transfer_size != []:
                            sns_transfer_size_gb = sum(sns_transfer_size) / len(sns_transfer_size)

                            # # Round to the nearest kb
                            # sns_transfer_size_gb = self._round_to_kb(
                            #     sns_transfer_size_gb, 1, False
                            # )

                            non_execution_info[sync_call_from_to_instance][
                                "sns_transfer_size_gb"
                            ] = sns_transfer_size_gb
                        else:
                            non_execution_info[sync_call_from_to_instance]["sns_transfer_size_gb"] = 0.0

    def _calculate_average_sync_table_and_sns_size(self, instance_summary: dict[str, Any]) -> None:
        for from_instance in instance_summary.values():
            for to_instance in from_instance.get("to_instance", {}).values():
                sync_sizes_gb = to_instance.get("sync_size_gb", [])
                if sync_sizes_gb:
                    average_sync_size = sum(sync_sizes_gb) / len(sync_sizes_gb)

                    # # Round to nearest 1 KB
                    # average_sync_size = self._round_to_kb(average_sync_size, 1, False)

                    to_instance["sync_size_gb"] = average_sync_size
                else:
                    del to_instance["sync_size_gb"]

                # Handle averaging the SNS only sizes
                sns_only_size_gb = to_instance.get("sns_only_size_gb", [])
                if sns_only_size_gb:
                    average_sns_only_size = sum(sns_only_size_gb) / len(sns_only_size_gb)

                    # # Round to nearest 1 KB
                    # average_sns_only_size = self._round_to_kb(average_sns_only_size, 1, False)

                    to_instance["sns_only_size_gb"] = average_sns_only_size
                else:
                    del to_instance["sns_only_size_gb"]

    # Best fit line approach.
    def _handle_missing_region_to_region_transmission_data(self, instance_summary: dict[str, Any]) -> None:
        for instance_val in instance_summary.values():  # pylint: disable=too-many-nested-blocks
            to_instances = instance_val.get("to_instance", {})
            for to_instance_val in to_instances.values():
                # Handle the missing region to region transmission data
                regions_to_regions = to_instance_val.get("regions_to_regions", {})
                for from_regions_information in regions_to_regions.values():
                    for to_region_information in from_regions_information.values():
                        # Calculate the best fit line for the data (Used in case of missing data)
                        transfer_size_to_transfer_latencies = to_region_information[
                            "transfer_size_gb_to_transfer_latencies_s"
                        ]

                        number_of_data_sizes = len(transfer_size_to_transfer_latencies)
                        if number_of_data_sizes == 0:
                            # Case where there are no data
                            continue

                        to_region_information["best_fit_line"] = self._calculate_best_fit_line(
                            transfer_size_to_transfer_latencies
                        )

    def _calculate_best_fit_line(self, transfer_size_to_transfer_latencies: dict[str, Any]) -> dict[str, Any]:
        number_of_data_sizes = len(transfer_size_to_transfer_latencies)

        # Calculate the average latency for each transfer size
        average_transfer_size_to_transfer_latencies = {}
        for size, latencies in transfer_size_to_transfer_latencies.items():
            if len(latencies) == 0:
                latencies.append(0.0)

            average_transfer_size_to_transfer_latencies[size] = sum(latencies) / len(latencies)

        # Calculate the averege transfer latency
        average_transfer_latency = sum(average_transfer_size_to_transfer_latencies.values()) / number_of_data_sizes

        slope = 0.0
        intercept = average_transfer_latency
        if number_of_data_sizes > 1:
            # Prepare the data
            transfer_sizes: list[float] = []
            transfer_latencies: list[float] = []

            for size, latencies in transfer_size_to_transfer_latencies.items():
                size_fl = float(size)
                for latency in latencies:
                    transfer_sizes.append(size_fl)
                    transfer_latencies.append(latency)

            # Convert to numpy arrays
            x = np.array(transfer_sizes)
            y = np.array(transfer_latencies)

            # Perform linear regression
            potential_slope, potential_intercept, _, _, _ = stats.linregress(x, y)

            # Check if the found slope and intercept
            # are somewhat reasonable
            if potential_intercept > 0.0 and potential_slope >= 0.0:
                slope = potential_slope
                intercept = potential_intercept

        # Save the best fit line and add some
        # limitations such as min and max latency
        best_fit_line = {
            "slope_s": slope,
            "intercept_s": intercept,
            "min_latency_s": average_transfer_latency * 0.7,
            "max_latency_s": average_transfer_latency * 1.3,
        }

        return best_fit_line

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
