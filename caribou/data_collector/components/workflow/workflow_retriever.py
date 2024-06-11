import json
from typing import Any, Optional

from caribou.common.constants import WORKFLOW_SUMMARY_TABLE
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

        start_hop_summary, instance_summary, runtime_samples = self._construct_summaries(summarized_workflow.get("logs", {}))

        return {
            "workflow_runtime_samples": runtime_samples,
            "daily_invocation_counts": summarized_workflow.get("daily_invocation_counts", {}),
            "daily_failure_counts": summarized_workflow.get("daily_failure_counts", {}),
            "start_hop_summary": start_hop_summary,
            "instance_summary": instance_summary,
        }

    def _construct_summaries(self, logs: list[dict[str, Any]]) -> tuple[dict[str, Any], dict[str, Any], list[float]]:
        start_hop_summary: dict[str, Any] = {}
        instance_summary: dict[str, Any] = {}
        runtime_samples: list[float] = []

        for log in logs:
            # Add to sample runtime
            workflow_runtime = log.get("runtime", None)
            if workflow_runtime is not None:
                runtime_samples.append(workflow_runtime)
            
            # Add to start hop summary
            self._extend_start_hop_summary(start_hop_summary, log)

            # Add to instance summary
            self._extend_instance_summary(instance_summary, log)

        # Perform post processing
        self._handle_missing_region_to_region_transmission_data(instance_summary)
        self._calculate_invocation_probability(instance_summary)
        self._reorganize_instance_summary(instance_summary)

        return start_hop_summary, instance_summary, runtime_samples

    def _extend_start_hop_summary(self, start_hop_summary: dict[str, Any], log: dict[str, Any]) -> None:
        start_hop_log: dict[str, Any] = log.get("start_hop_info", None)
        if start_hop_log:
            start_hop_destination = start_hop_log.get("destination", None)
            if start_hop_destination:
                if start_hop_destination not in start_hop_summary:
                    start_hop_summary[start_hop_destination] = {}

                start_hop_data_transfer_size = float(start_hop_log.get("data_transfer_size", 0.0))

                if start_hop_data_transfer_size not in start_hop_summary[start_hop_destination]:
                    start_hop_summary[start_hop_destination][start_hop_data_transfer_size] = []
                start_hop_latency = start_hop_log.get("latency", None)
                if start_hop_latency is not None:
                    start_hop_summary[start_hop_destination][start_hop_data_transfer_size].append(start_hop_latency)

    def _extend_instance_summary(  # pylint: disable=too-many-branches
        self, instance_summary: dict[str, Any], log: dict[str, Any]
    ) -> None:
        self._handle_execution_data(log, instance_summary)

        self._handle_region_to_region_transmission(log, instance_summary)

        self._handle_non_executions(log, instance_summary)

    def _handle_execution_data(self, log: dict[str, Any], instance_summary: dict[str, Any]) -> None:
        for execution_information in log["execution_data"]:
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
                "duration": execution_information["duration"],
                "data_transfer_during_execution": execution_information["data_transfer_during_execution"],
                "download_size": execution_information.get("download_information", {}).get("download_size", 0.0),
                "successor_invocations": {},
            }
            
            successor_data: Optional[dict[str, Any]] = execution_information.get("successor_data", None)
            if successor_data is not None:
                for successor, successor_data in successor_data.items():
                    execution_data["successor_invocations"][successor] = {
                        "invocation_time_from_function_start": successor_data["invocation_time_from_function_start"],
                    }
                    instance_summary[instance]["executions"]["successor_instances"].add(successor)

            instance_summary[instance]["executions"]["at_region"][provider_region].append(execution_data)

    def _reorganize_instance_summary(self, instance_summary: dict[str, Any]) -> None:
        for instance_val in instance_summary.values():
            execution_data = instance_val.get("executions", {})
            if execution_data:
                # Get list of all successor instances
                successor_instances = list(instance_val["executions"]["successor_instances"])
                # Now remove the successor instances from the dictionary
                del instance_val["executions"]["successor_instances"]

                # Get translation of successor instances
                # Basically index 0-2 is reserved, then every other instance is assigned a new index
                index_translation = {
                    "duration": 0,
                    "data_transfer_during_execution": 1,
                    "download_size": 2,
                }
                for index, successor_instance in enumerate(successor_instances):
                    index_translation[successor_instance] = index + 3

                # Add the index translation to the instance_val
                instance_val["executions"]["index_translation"] = index_translation

                # Now translate the at_region data to be in a list of the
                # form with values stored in the order of index_translation
                for region, execution_data in instance_val["executions"]["at_region"].items():
                    reformatted_data = []
                    
                    for execution in execution_data:
                        new_execution = [None] * len(index_translation)
                        new_execution[0] = execution["duration"]
                        new_execution[1] = execution["data_transfer_during_execution"]
                        new_execution[2] = execution["download_size"]
                        for successor, successor_data in execution["successor_invocations"].items():
                            new_execution[index_translation[successor]] = successor_data["invocation_time_from_function_start"]

                        reformatted_data.append(new_execution)

                    instance_val["executions"]["at_region"][region] = reformatted_data

    def _handle_region_to_region_transmission(self, log: dict[str, Any], instance_summary: dict[str, Any]) -> None:
        for data in log["transmission_data"]:
            from_instance = data["from_instance"]
            to_instance = data["to_instance"]
            from_region = data["from_region"]
            to_region = data["to_region"]
            successor_invoked = data["successor_invoked"]
            from_direct_successor = data["from_direct_successor"]

            # Add an entry for the transfer size
            # TODO: Right now we are making simple assumption that
            # since wrapper should be small, the data transfer is
            # always the sum of the wrapper and potential data upload size.
            # In the future this may need to be more sophisticated.
            transmission_data_transfer_size = float(data["transmission_size"])
            # Check if the transmission data also contain sync_information
            # Denoting if it uploads or recieves data from synchronization
            sync_information = data.get("sync_information", None)
            if (sync_information is not None):
                # Right now we are making an assumption that data transfer size
                # Is the sum of the wrapper data transfer size and the upload size
                upload_size: float = sync_information.get("upload_size", 0.0)
                transmission_data_transfer_size += upload_size

            if from_direct_successor:
                # This is the case where the transmission is from a direct successor

                # Create the missing dictionary entries (Common)
                if from_instance not in instance_summary:
                    instance_summary[from_instance] = {}
                if 'to_instance' not in instance_summary[from_instance]:
                    instance_summary[from_instance]['to_instance'] = {}
                if to_instance not in instance_summary[from_instance]["to_instance"]:
                    instance_summary[from_instance]["to_instance"][to_instance] = {
                        "invoked": 0,
                        "non_executions": 0,
                        "invocation_probability": 0.0,
                        "transfer_sizes": [],
                        "regions_to_regions": {},
                    }
                
                # Increment invoked count (Even if not directly invoked)
                instance_summary[from_instance]["to_instance"][to_instance]["invoked"] += 1

                # Handle the transmission data
                ## First create the missing dictionary entries
                if from_region not in instance_summary[from_instance]["to_instance"][to_instance]["regions_to_regions"]:
                    instance_summary[from_instance]["to_instance"][to_instance]["regions_to_regions"][from_region] = {}
                if (to_region not in instance_summary[from_instance]["to_instance"][to_instance]["regions_to_regions"][from_region]):
                    instance_summary[from_instance]["to_instance"][to_instance]["regions_to_regions"][from_region][to_region] = {
                        "transfer_size_to_transfer_latencies": {},
                    }

                instance_summary[from_instance]["to_instance"][to_instance]["transfer_sizes"].append(transmission_data_transfer_size)
                
                # Add an entry for the transfer latency
                # (Only for cases where successor_invoked is True)
                # Else we do not have the transfer latency for this
                # node as it is not directly invoked
                if successor_invoked:
                    transmission_data_transfer_size_str = str(transmission_data_transfer_size)
                    if (transmission_data_transfer_size_str not in instance_summary[from_instance]["to_instance"][to_instance]["regions_to_regions"][from_region][to_region]["transfer_size_to_transfer_latencies"]):
                        instance_summary[from_instance]["to_instance"][to_instance]["regions_to_regions"][from_region][to_region]["transfer_size_to_transfer_latencies"][transmission_data_transfer_size_str] = []
                    instance_summary[from_instance]["to_instance"][to_instance]["regions_to_regions"][from_region][to_region]["transfer_size_to_transfer_latencies"][transmission_data_transfer_size_str].append(
                        data["transmission_latency"]
                    )
            else:
                # TODO: Handle this scenerio. This is the case where the transmission is not from a direct successor
                # Aka via non-execution, a node calls some potentially far descendant
                # This is the case where the transmission is not from a direct successor
                # Aka for a non-execution, it invokes some descendant
                # This may need to be simulated for non-execution calls
                pass

    def _handle_non_executions(self, log: dict[str, Any], instance_summary: dict[str, Any]) -> None:
        non_executions = log.get("non_executions", {})
        for caller, non_execution in non_executions.items():
            # Create the missing dictionary entries
            if caller not in instance_summary:
                instance_summary[caller] = {}
            if 'to_instance' not in instance_summary[caller]:
                instance_summary[caller]['to_instance'] = {}
            for callee, count in non_execution.items():
                if callee not in instance_summary[caller]["to_instance"]:
                    instance_summary[caller]["to_instance"][callee] = {
                        "invoked": 0,
                        "non_executions": 0,
                        "invocation_probability": 0.0,
                        "regions_to_regions": {},
                    }

                # Append the number of non-executions
                instance_summary[caller]["to_instance"][callee]["non_executions"] += count

    def _calculate_invocation_probability(self, instance_summary: dict[str, Any]) -> None:
        for from_instance in instance_summary.values():
            for caller_callee_data in from_instance.get("to_instance", {}).values():
                caller_callee_data["invocation_probability"] = caller_callee_data["invoked"] / (
                    caller_callee_data["invoked"] + caller_callee_data["non_executions"]
                )

    def _handle_missing_region_to_region_transmission_data(self, instance_summary: dict[str, Any]) -> None:
        for instance_val in instance_summary.values():  # pylint: disable=too-many-nested-blocks
            to_instances = instance_val.get("to_instance", {})
            for to_instance_val in to_instances.values():
                all_transfer_sizes = set(to_instance_val.get("transfer_sizes", []))
                regions_to_regions = to_instance_val.get("regions_to_regions", {})
                for from_regions_information in regions_to_regions.values():
                    # Calculate global averages for each size
                    global_avg_latency_per_size = {}
                    for size in all_transfer_sizes:
                        total_latencies = []
                        for to_region_information in from_regions_information.values():
                            latencies = to_region_information["transfer_size_to_transfer_latencies"].get(size)
                            if latencies:
                                total_latencies.extend(latencies)
                        if total_latencies:
                            global_avg_latency_per_size[size] = sum(total_latencies) / len(total_latencies)

                    # Scale missing latencies based on the common transfer size (if available)
                    for to_region_information in from_regions_information.values():
                        existing_sizes = {
                            float(data) for data in to_region_information["transfer_size_to_transfer_latencies"].keys()
                        }
                        
                        # Only scale if there are missing sizes
                        # And if there are existing sizes
                        if existing_sizes:
                            missing_sizes = all_transfer_sizes - existing_sizes

                            for missing_size in missing_sizes:
                                # Find the nearest size for which we have data
                                try:
                                    nearest_size = min(
                                        to_region_information["transfer_size_to_transfer_latencies"].keys(),
                                        key=lambda x, missing_size=missing_size: abs(float(x) - float(missing_size)),  # type: ignore  # pylint: disable=line-too-long
                                    )
                                    scaling_factor = (
                                        global_avg_latency_per_size[missing_size]
                                        / global_avg_latency_per_size[nearest_size]
                                        if nearest_size in global_avg_latency_per_size
                                        else 1
                                    )

                                    scaled_latencies = [
                                        latency * scaling_factor
                                        for latency in to_region_information["transfer_size_to_transfer_latencies"][
                                            nearest_size
                                        ]
                                    ]
                                    to_region_information["transfer_size_to_transfer_latencies"][
                                        missing_size
                                    ] = scaled_latencies
                                except KeyError:
                                    pass