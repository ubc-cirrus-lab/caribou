import json
import re
from datetime import datetime, timedelta
from typing import Any, Optional

from caribou.common.constants import (
    FORGETTING_NUMBER,
    FORGETTING_TIME_DAYS,
    GLOBAL_TIME_ZONE,
    KEEP_ALIVE_DATA_COUNT,
    LOG_VERSION,
    TIME_FORMAT,
    TIME_FORMAT_DAYS,
    WORKFLOW_SUMMARY_TABLE,
)
from caribou.common.models.remote_client.remote_client import RemoteClient
from caribou.common.models.remote_client.remote_client_factory import RemoteClientFactory
from caribou.syncers.indirect_transmission_data import IndirectTransmissionData
from caribou.syncers.workflow_run_sample import WorkflowRunSample


class LogSyncWorkflow:  # pylint: disable=too-many-instance-attributes
    def __init__(
        self,
        workflow_id: str,
        region_clients: dict[tuple[str, str], RemoteClient],
        deployment_manager_config_str: str,
        time_intervals_to_sync: list[tuple[datetime, datetime]],
        workflow_summary_client: RemoteClient,
        previous_data: dict,
    ) -> None:
        self.workflow_id = workflow_id
        self._collected_logs: dict[str, WorkflowRunSample] = {}
        self._daily_invocation_set: dict[str, set[str]] = {}
        self._time_intervals_to_sync: list[tuple[datetime, datetime]] = time_intervals_to_sync
        self._tainted_cold_start_samples: set[str] = set()
        self._blacklisted_run_ids: set[str] = set()
        self._forgetting = False
        self._region_clients = region_clients
        self._workflow_summary_client = workflow_summary_client
        self._previous_data = previous_data
        self._deployed_regions: dict[str, dict[str, Any]] = {}
        self._load_information(deployment_manager_config_str)
        self._insights_logs: dict[str, Any] = {}

        self._existing_data: dict[str, Any] = {
            "execution_instance_region": {},
            "transmission_from_instance_to_instance_region": {},
        }

        self._forgetting_number = FORGETTING_NUMBER

    def _load_information(self, deployment_manager_config_str: str) -> None:
        deployment_manager_config = json.loads(deployment_manager_config_str)
        deployed_regions_str = deployment_manager_config.get("deployed_regions", "{}")
        self._deployed_regions = json.loads(deployed_regions_str)

    def _get_remote_client(self, provider_region: dict[str, str]) -> RemoteClient:
        if (provider_region["provider"], provider_region["region"]) not in self._region_clients:
            self._region_clients[
                (provider_region["provider"], provider_region["region"])
            ] = RemoteClientFactory.get_remote_client(provider_region["provider"], provider_region["region"])
        return self._region_clients[(provider_region["provider"], provider_region["region"])]

    def sync_workflow(self) -> None:
        self._sync_logs()
        data_for_upload: str = self._prepare_data_for_upload(self._previous_data)
        # self._upload_data(data_for_upload)
        print(json.dumps(json.loads(data_for_upload), indent=4))

    def _upload_data(self, data_for_upload: str) -> None:
        self._workflow_summary_client.update_value_in_table(
            WORKFLOW_SUMMARY_TABLE,
            self.workflow_id,
            data_for_upload,
        )

    def _sync_logs(self) -> None:
        for function_physical_instance, instance_information in self._deployed_regions.items():
            provider_region = instance_information["deploy_region"]

            for time_from, time_to in self._time_intervals_to_sync:
                self._process_logs_for_instance_for_one_region(
                    function_physical_instance, provider_region, time_from, time_to
                )
        self._check_to_forget()

    def _process_logs_for_instance_for_one_region(
        self, functions_instance: str, provider_region: dict[str, str], time_from: datetime, time_to: datetime
    ) -> None:
        remote_client = self._get_remote_client(provider_region)
        logs = remote_client.get_logs_between(functions_instance, time_from, time_to)
        if len(logs) == 0:
            return

        # Lambda insight logs may not available at the same time as the lambda logs
        # so we need to fetch logs from a wider time range (30 minutes before and after the lambda logs,
        # could be recalibrated if needed)
        lambda_insights_logs = remote_client.get_insights_logs_between(
            functions_instance, time_from - timedelta(minutes=30), time_to + timedelta(minutes=30)
        )
        self._setup_lambda_insights(lambda_insights_logs)

        for log in logs:
            # Only process logs associated with our framework
            # Which are marked with the [CARIBOU] tag
            # Or are the AWS Lambda report logs (Just the end of the execution)
            if log.startswith("[CARIBOU]") or log.startswith("REPORT RequestId:"):
                self._process_log_entry(log, provider_region, time_to)

    def _setup_lambda_insights(self, logs: list[str]) -> None:
        # Clear the lambda insights logs
        self._insights_logs = {}

        # Process the lambda insights logs
        for log in logs:
            log_dict = json.loads(log)

            request_id = log_dict.get("request_id", None)
            if request_id:
                important_entries = [
                    "cold_start",
                    "used_memory_max",
                    "total_memory",
                    "memory_utilization",
                    "cpu_total_time",
                    "duration",
                    "rx_bytes",
                    "tx_bytes",
                    "total_network",
                ]
                for entry in important_entries:
                    if entry in log_dict:
                        # Do unit conversion if needed
                        # For duration and cpu_total_time, we convert from ms to s
                        if entry in ["duration", "cpu_total_time"]:
                            log_dict[entry] = log_dict[entry] / 1000

                        # For "rx_bytes", "tx_bytes", and total_network, we convert from bytes to GB
                        if entry in ["rx_bytes", "tx_bytes", "total_network"]:
                            log_dict[entry] = log_dict[entry] / (1024**3)  # Convert bytes to GB

                        if request_id not in self._insights_logs:
                            self._insights_logs[request_id] = {}
                        self._insights_logs[request_id][entry] = log_dict[entry]

    def _process_log_entry(self, log_entry: str, provider_region: dict[str, str], time_to: datetime) -> None:
        # If this log entry contains an init duration, then this run has incurred a cold start.
        # We taint the sample with this information.
        # Those logs starts with "REPORT" and contains "Init Duration"
        if log_entry.startswith("REPORT") and "Init Duration" in log_entry:
            request_id = self._extract_from_string(log_entry, r"RequestId: (.*?)\t")
            if request_id:
                self._tainted_cold_start_samples.add(request_id)

        # Ensure that the log entry is a valid log entry and has the correct version
        # Those logs starts with "[CARIBOU]" and contains "LOG_VERSION"
        if not log_entry.startswith("[CARIBOU]") and f"LOG_VERSION ({LOG_VERSION})" not in log_entry:
            return

        # At this point, every log entry should be from our framework
        # And that it should contain the correct log version

        # Extract the run_id and log_time from the log entry
        run_id = self._extract_from_string(log_entry, r"RUN_ID \((.*?)\)")
        if not isinstance(run_id, str):
            raise ValueError(f"Invalid run_id: {run_id}")

        log_time = self._extract_from_string(log_entry, r"TIME \((.*?)\)")
        log_time_dt = None
        if log_time:
            log_time_dt = datetime.strptime(log_time, TIME_FORMAT)
        if not isinstance(log_time_dt, datetime):
            raise ValueError(f"Invalid log time: {log_time}")

        log_day_str = log_time_dt.strftime(TIME_FORMAT_DAYS)
        if log_day_str not in self._daily_invocation_set:
            self._daily_invocation_set[log_day_str] = set()
        self._daily_invocation_set[log_day_str].add(run_id)

        # If we have collected enough logs, we can start to forget
        # This also checks if we have any tainted cold starts in the logs
        if len(self._collected_logs) >= self._forgetting_number and not self._forgetting:
            self._check_to_forget()

        if run_id not in self._collected_logs:
            # If we don't need to actually load the log, we can can return here
            # We still need to collect the other logs as they might contain
            # information about the already collected logs
            if self._forgetting or run_id in self._blacklisted_run_ids:
                return

            # If we haven't collected this log yet, we create a new sample
            self._collected_logs[run_id] = WorkflowRunSample(run_id)

        workflow_run_sample = self._collected_logs[run_id]
        workflow_run_sample.update_log_end_time(log_time_dt)

        # Extract the request_id from the log entry
        parts = log_entry.split("\t")
        request_id = parts[2]
        workflow_run_sample.request_ids.add(request_id)

        self._handle_system_log_messages(
            log_entry, run_id, workflow_run_sample, provider_region, log_time_dt, request_id, time_to
        )

    # pylint: disable=too-many-branches
    def _handle_system_log_messages(
        self,
        log_entry: str,
        run_id: str,
        workflow_run_sample: WorkflowRunSample,
        provider_region: dict[str, str],
        log_time: datetime,
        request_id: str,
        time_to: datetime,
    ) -> None:
        # Extract the message from the log entry
        pattern = re.compile(r"MESSAGE \((.*?)\) LOG_VERSION")
        match = re.search(pattern, log_entry)
        if match:
            message = match.group(1)

            # Now all log should start with one of the following:
            if message.startswith("ENTRY_POINT"):
                # Only care about the logs when the time is before the time_to interval
                # As thats when the new workflow run starts
                if log_time < time_to:
                    self._extract_entry_point_log(workflow_run_sample, message, provider_region, log_time)
                else:
                    # Blacklist the run_id as we don't need to collect more logs for it
                    del self._collected_logs[run_id]
                    self._blacklisted_run_ids.add(run_id)
            elif message.startswith("INVOKED"):
                self._extract_invoked_logs(workflow_run_sample, message, provider_region, log_time)
            elif message.startswith("EXECUTED"):
                self._extract_executed_logs(workflow_run_sample, message, provider_region, request_id)
            elif message.startswith("INVOKING_SUCCESSOR"):
                self._extract_invoking_successor_logs(
                    workflow_run_sample,
                    message,
                    provider_region,
                    log_time,
                )
            elif message.startswith("CONDITIONAL_NON_EXECUTION"):  # TODO: CONDITIONAL NON_EXECUTION NOT YET IMPLEMENTED
                self._extract_conditional_non_execution_logs(workflow_run_sample, message)
            elif message.startswith("USED_CPU_MODEL"):
                self._extract_cpu_model(workflow_run_sample, message)
            elif message.startswith("UPLOAD_DATA_TO_SYNC_TABLE"):
                self._extract_upload_data_to_sync_table(workflow_run_sample, message)
            elif message.startswith("DOWNLOAD_DATA_FROM_SYNC_TABLE"):
                self._extract_download_data_from_sync_table(workflow_run_sample, message)
            else:
                print("The following CARIBOU messages were untracked:", message)
        else:
            print("WARNING: No matches! Invalid PATTERN for log:", log_entry)

    def _extract_entry_point_log(
        self,
        workflow_run_sample: WorkflowRunSample,
        log_entry: str,
        provider_region: dict[str, str],
        log_time: datetime,
    ) -> None:
        workflow_run_sample.log_start_time = log_time
        workflow_run_sample.start_hop_destination = provider_region

        # Extract the instance name of the start hop from the log entry
        function_executed = self._extract_from_string(log_entry, r"INSTANCE \((.*?)\)")
        if not isinstance(function_executed, str):
            raise ValueError(f"Invalid function_executed: {function_executed}")
        workflow_run_sample.start_hop_instance_id = function_executed

        # Extract the start hop latency from the log entry
        start_hop_latency = self._extract_from_string(log_entry, r"INIT_LATENCY \((.*?)\)")
        if start_hop_latency and start_hop_latency != "N/A":
            start_hop_latency_fl: float = float(start_hop_latency)
            workflow_run_sample.start_hop_latency = start_hop_latency_fl

        # Extract the start hop latency and data transfer size from the log entry
        data_transfer_size = self._extract_from_string(log_entry, r"PAYLOAD_SIZE \((.*?)\)")
        if data_transfer_size:
            data_transfer_size_fl: float = float(data_transfer_size)
            workflow_run_sample.start_hop_data_transfer_size = data_transfer_size_fl

        # Extract the workflow placement payload size from the log entry
        workflow_placement_decision_size = self._extract_from_string(
            log_entry, r"WORKFLOW_PLACEMENT_DECISION_SIZE \((.*?)\)"
        )
        if workflow_placement_decision_size:
            workflow_placement_decision_size_fl: float = float(workflow_placement_decision_size)
            workflow_run_sample.start_hop_wpd_data_size = workflow_placement_decision_size_fl

        # Extract the coonsumed read capacity to extract this payload size from the log entry
        consumed_read_capacity = self._extract_from_string(log_entry, r"CONSUMED_READ_CAPACITY \((.*?)\)")
        if consumed_read_capacity:
            consumed_read_capacity_fl = float(consumed_read_capacity)
            workflow_run_sample.start_hop_wpd_consumed_read_capacity = consumed_read_capacity_fl

    def _extract_invoked_logs(
        self,
        workflow_run_sample: WorkflowRunSample,
        log_entry: str,
        provider_region: dict[str, str],
        log_time: datetime,
    ) -> None:
        taint = self._extract_from_string(log_entry, r"TAINT \((.*?)\)")
        if not isinstance(taint, str):
            raise ValueError(f"Invalid taint: {taint}")

        transmission_data = workflow_run_sample.get_transmission_data(taint)
        transmission_data.to_region = provider_region
        transmission_data.transmission_end_time = log_time

    def _extract_executed_logs(
        self, workflow_run_sample: WorkflowRunSample, log_entry: str, provider_region: dict[str, str], request_id: str
    ) -> None:
        function_executed = self._extract_from_string(log_entry, r"INSTANCE \((.*?)\)")
        if not isinstance(function_executed, str):
            raise ValueError(f"Invalid function_executed: {function_executed}")

        duration = self._extract_from_string(log_entry, r"EXECUTION_TIME \((.*?)\)")
        if duration:
            duration = float(duration)  # type: ignore
        if not isinstance(duration, float):
            raise ValueError(f"Invalid duration: {duration}")

        provider_region_str = provider_region["provider"] + ":" + provider_region["region"]

        # Add the entry to the execution data if it doesn't exist
        if function_executed not in workflow_run_sample.execution_data:
            workflow_run_sample.execution_data[function_executed] = {}

        workflow_run_sample.execution_data[function_executed]["latency"] = duration
        workflow_run_sample.execution_data[function_executed]["provider_region"] = provider_region_str
        workflow_run_sample.execution_data[function_executed]["insights"] = self._insights_logs.get(request_id, {})

    def _extract_invoking_successor_logs(
        self,
        workflow_run_sample: WorkflowRunSample,
        log_entry: str,
        provider_region: dict[str, str],
        log_time: datetime,
    ) -> None:
        taint = self._extract_from_string(log_entry, r"TAINT \((.*?)\)")
        if not isinstance(taint, str):
            raise ValueError(f"Invalid taint: {taint}")

        caller_function = self._extract_from_string(log_entry, r"INSTANCE \((.*?)\)")
        if not isinstance(caller_function, str):
            raise ValueError(f"Invalid caller_function: {caller_function}")

        callee_function = self._extract_from_string(log_entry, r"SUCCESSOR \((.*?)\)")
        if not isinstance(callee_function, str):
            raise ValueError(f"Invalid callee_function: {callee_function}")

        data_transfer_size = self._extract_from_string(log_entry, r"PAYLOAD_SIZE \((.*?)\)")
        if data_transfer_size:
            data_transfer_size = float(data_transfer_size)  # type: ignore
        if not isinstance(data_transfer_size, float):
            raise ValueError(f"Invalid data_transfer_size: {data_transfer_size}")

        transmission_data = workflow_run_sample.get_transmission_data(taint)
        transmission_data.from_region = provider_region
        transmission_data.from_instance = caller_function
        transmission_data.to_instance = callee_function
        transmission_data.transmission_start_time = log_time
        transmission_data.transmission_size = data_transfer_size

    def _extract_conditional_non_execution_logs(self, workflow_run_sample: WorkflowRunSample, log_entry: str) -> None:
        caller_function = self._extract_from_string(log_entry, r"INSTANCE \((.*?)\)")
        if not isinstance(caller_function, str):
            raise ValueError(f"Invalid caller_function: {caller_function}")

        callee_function = self._extract_from_string(log_entry, r"SUCCESSOR \((.*?)\)")
        if not isinstance(callee_function, str):
            raise ValueError(f"Invalid callee_function: {callee_function}")

        consumed_write_capacity = self._extract_from_string(log_entry, r"CONSUMED_WRITE_CAPACITY \((.*?)\)")
        if consumed_write_capacity:
            consumed_write_capacity = float(consumed_write_capacity)  # type: ignore
        if not isinstance(consumed_write_capacity, float):
            raise ValueError(f"Invalid consumed_write_capacity: {consumed_write_capacity}")

        # TODO: NEED TO LOOK INTO HOW TO ADDRESS THIS,
        # MAYBE WE WOULD NOT SET THE DESTINATION HERE
        destination_provider = self._extract_from_string(log_entry, r"PROVIDER \((.*?)\)")
        if not isinstance(destination_provider, str):
            raise ValueError(f"Invalid destination_provider: {destination_provider}")

        destination_region = self._extract_from_string(log_entry, r"REGION \((.*?)\)")
        if not isinstance(destination_region, str):
            raise ValueError(f"Invalid destination_region: {destination_region}")

        print(f"caller_function: {caller_function}")
        print(f"callee_function: {callee_function}")
        print(f"consumed_write_capacity: {consumed_write_capacity}")
        print(f"destination_provider: {destination_provider}")
        print(f"destination_region: {destination_region}")

        if caller_function not in workflow_run_sample.non_executions:
            workflow_run_sample.non_executions[caller_function] = {}
        if callee_function not in workflow_run_sample.non_executions[caller_function]:
            workflow_run_sample.non_executions[caller_function][callee_function] = 0
        workflow_run_sample.non_executions[caller_function][callee_function] += 1

    def _extract_cpu_model(self, workflow_run_sample: WorkflowRunSample, log_entry: str) -> None:
        function_executed = self._extract_from_string(log_entry, r"INSTANCE \((.*?)\)")
        if not isinstance(function_executed, str):
            raise ValueError(f"Invalid function_executed: {function_executed}")

        cpu_model = self._extract_from_string(log_entry, r"CPU_MODEL \((.*?)\)")
        if not isinstance(cpu_model, str):
            raise ValueError(f"Invalid cpu_model: {cpu_model}")
        cpu_model = cpu_model.replace("<", "(").replace(">", ")")

        # Add the entry to the execution data if it doesn't exist
        if function_executed not in workflow_run_sample.execution_data:
            workflow_run_sample.execution_data[function_executed] = {}

        workflow_run_sample.execution_data[function_executed]["cpu_model"] = cpu_model
        workflow_run_sample.cpu_models.add(cpu_model)

    def _extract_upload_data_to_sync_table(self, workflow_run_sample: WorkflowRunSample, log_entry: str) -> None:
        caller_function = self._extract_from_string(log_entry, r"INSTANCE \((.*?)\)")
        if not isinstance(caller_function, str):
            raise ValueError(f"Invalid caller_function: {caller_function}")

        callee_function = self._extract_from_string(log_entry, r"SUCCESSOR \((.*?)\)")
        if not isinstance(callee_function, str):
            raise ValueError(f"Invalid callee_function: {callee_function}")

        upload_size = self._extract_from_string(log_entry, r"UPLOAD_SIZE \((.*?)\)")
        if upload_size:
            upload_size = float(upload_size)  # type: ignore
        if not isinstance(upload_size, float):
            raise ValueError(f"Invalid upload_size: {upload_size}")

        upload_rtt = self._extract_from_string(log_entry, r"UPLOAD_RTT \((.*?)\)")
        if upload_rtt:
            upload_rtt = float(upload_rtt)  # type: ignore
        if not isinstance(upload_rtt, float):
            raise ValueError(f"Invalid upload_rtt: {upload_rtt}")

        potential_payload_size = self._extract_from_string(log_entry, r"PAYLOAD_SIZE \((.*?)\)")
        if potential_payload_size:
            potential_payload_size = float(potential_payload_size)  # type: ignore
        if not isinstance(potential_payload_size, float):
            raise ValueError(f"Invalid potential_payload_size: {potential_payload_size}")

        consumed_write_capacity = self._extract_from_string(log_entry, r"CONSUMED_WRITE_CAPACITY \((.*?)\)")
        if consumed_write_capacity:
            consumed_write_capacity = float(consumed_write_capacity)  # type: ignore
        if not isinstance(consumed_write_capacity, float):
            raise ValueError(f"Invalid consumed_write_capacity: {consumed_write_capacity}")

        destination_provider = self._extract_from_string(log_entry, r"PROVIDER \((.*?)\)")
        if not isinstance(destination_provider, str):
            raise ValueError(f"Invalid destination_provider: {destination_provider}")

        destination_region = self._extract_from_string(log_entry, r"REGION \((.*?)\)")
        if not isinstance(destination_region, str):
            raise ValueError(f"Invalid destination_region: {destination_region}")

        # Add the entry to the execution data if it doesn't exist
        if caller_function not in workflow_run_sample.execution_data:
            workflow_run_sample.execution_data[caller_function] = {}

        # Add to execution summary
        workflow_run_sample.execution_data[caller_function]["sync_data_upload_size"] = upload_size
        workflow_run_sample.execution_data[caller_function][
            "sync_data_consumed_write_capacity"
        ] = consumed_write_capacity

        # Add to indirect transmission data
        workflow_run_sample.indirect_transmission_data.append(
            IndirectTransmissionData(
                transmission_start_time=workflow_run_sample.log_start_time,
                upload_rtt=upload_rtt,
                potential_transmission_size=potential_payload_size,
                from_instance=caller_function,
                to_instance=callee_function,
                from_region=workflow_run_sample.start_hop_destination,
                to_region={"provider": destination_provider, "region": destination_region},
            )
        )

    def _extract_download_data_from_sync_table(self, workflow_run_sample: WorkflowRunSample, log_entry: str) -> None:
        function_executed = self._extract_from_string(log_entry, r"INSTANCE \((.*?)\)")
        if not isinstance(function_executed, str):
            raise ValueError(f"Invalid function_executed: {function_executed}")

        download_size = self._extract_from_string(log_entry, r"DOWNLOAD_SIZE \((.*?)\)")
        if download_size:
            download_size = float(download_size)  # type: ignore
        if not isinstance(download_size, float):
            raise ValueError(f"Invalid download_size: {download_size}")

        consumed_read_capacity = self._extract_from_string(log_entry, r"CONSUMED_READ_CAPACITY \((.*?)\)")
        if consumed_read_capacity:
            consumed_read_capacity = float(consumed_read_capacity)
        if not isinstance(consumed_read_capacity, float):
            raise ValueError(f"Invalid consumed_read_capacity: {consumed_read_capacity}")

        # Add the entry to the execution data if it doesn't exist
        if function_executed not in workflow_run_sample.execution_data:
            workflow_run_sample.execution_data[function_executed] = {}

        workflow_run_sample.execution_data[function_executed]["sync_data_download_size"] = download_size
        workflow_run_sample.execution_data[function_executed][
            "sync_data_consumed_read_capacity"
        ] = consumed_read_capacity

    def _check_to_forget(self) -> None:
        # We need to check if the logs we have contain no tainted cold starts
        # If they do, we delete the logs and need to continue to collect
        run_ids = set(self._collected_logs.keys())
        for run_id in run_ids:
            workflow_run_sample = self._collected_logs[run_id]
            if workflow_run_sample.request_ids & self._tainted_cold_start_samples:
                del self._collected_logs[run_id]
                self._blacklisted_run_ids.add(run_id)

        # If the size of the collected logs is still the same, we can start to forget
        if len(self._collected_logs) >= self._forgetting_number:
            self._forgetting = True

    def _extract_from_string(self, log_entry: str, regex: str) -> Optional[str]:
        match = re.search(regex, log_entry)
        return match.group(1) if match else None

    def _prepare_data_for_upload(self, previous_data: dict) -> str:
        previous_daily_invocation_counts = previous_data.get("daily_invocation_counts", {})

        self._filter_daily_invocation_counts(previous_daily_invocation_counts)
        self._merge_daily_invocation_counts(previous_daily_invocation_counts)

        daily_invocation_counts = previous_daily_invocation_counts

        collected_logs: list[dict[str, Any]] = self._format_collected_logs()
        self._fill_up_collected_logs(collected_logs, previous_data)

        workflow_runtime_samples: list[float] = [collected_logs["runtime"] for collected_logs in collected_logs]

        data_to_upload = {
            "daily_invocation_counts": daily_invocation_counts,
            "logs": collected_logs,
            "workflow_runtime_samples": workflow_runtime_samples,
            "last_sync_time": self._time_intervals_to_sync[-1][1].strftime(TIME_FORMAT),
        }
        print(data_to_upload)
        return json.dumps(data_to_upload)

    def _fill_up_collected_logs(self, collected_logs: list[dict[str, Any]], previous_data: dict) -> None:
        oldest_allowed_date = datetime.now(GLOBAL_TIME_ZONE) - timedelta(days=FORGETTING_TIME_DAYS)
        previous_logs = previous_data.get("logs", [])
        # Iterate over the previous logs in reverse order,
        for previous_log in reversed(previous_logs):
            # Do this until we either exceed the forgetting number or run out of previous logs
            if len(collected_logs) >= self._forgetting_number:
                self._selectively_add_previous_logs(collected_logs, previous_log)
                continue
            log_start_time = datetime.strptime(previous_log["start_time"], TIME_FORMAT)
            # check if the log start time is younger
            # than the oldest allowed date and add it to the collected logs if it is
            if log_start_time > oldest_allowed_date:
                # Prepend the previous logs to the collected logs to ensure that the oldest logs are at the front
                collected_logs.insert(0, previous_log)
            # If the log start time is older than the oldest allowed date, we can break
            # as all the logs after this will be older
            else:
                break

    def _selectively_add_previous_logs(
        self, collected_logs: list[dict[str, Any]], previous_log: dict[str, Any]
    ) -> None:
        has_missing_execution_instance_region = self._check_for_missing_execution_instance_region(previous_log)
        has_missing_transmission_from_instance_to_instance_region = (
            self._check_for_missing_transmission_from_instance_to_instance_region(previous_log)
        )
        # If the log contains information that is not already in the collected logs, we add it
        if has_missing_execution_instance_region or has_missing_transmission_from_instance_to_instance_region:
            collected_logs.insert(0, previous_log)

    def _check_for_missing_execution_instance_region(self, previous_log: dict[str, Any]) -> bool:
        has_missing_information = False
        for function, execution_data in previous_log["execution_data"].items():
            if function not in self._existing_data["execution_instance_region"]:
                has_missing_information = True
                self._existing_data["execution_instance_region"][function] = {}
            provider_region = execution_data["provider_region"]
            if provider_region not in self._existing_data["execution_instance_region"][function]:
                has_missing_information = True
                self._existing_data["execution_instance_region"][function][provider_region] = 0
            self._existing_data["execution_instance_region"][function][provider_region] += 1

            if self._existing_data["execution_instance_region"][function][provider_region] < KEEP_ALIVE_DATA_COUNT:
                has_missing_information = True

        return has_missing_information

    def _check_for_missing_transmission_from_instance_to_instance_region(self, previous_log: dict[str, Any]) -> bool:
        has_missing_information = False
        for transmission_data in previous_log["direct_transmission_data"]:
            from_instance = transmission_data["from_instance"]
            to_instance = transmission_data["to_instance"]
            from_region_str = (
                transmission_data["from_region"]["provider"] + ":" + transmission_data["from_region"]["region"]
            )
            to_region_str = transmission_data["to_region"]["provider"] + ":" + transmission_data["to_region"]["region"]
            if from_instance not in self._existing_data["transmission_from_instance_to_instance_region"]:
                has_missing_information = True
                self._existing_data["transmission_from_instance_to_instance_region"][from_instance] = {}
            if to_instance not in self._existing_data["transmission_from_instance_to_instance_region"][from_instance]:
                has_missing_information = True
                self._existing_data["transmission_from_instance_to_instance_region"][from_instance][to_instance] = {}
            if (
                from_region_str
                not in self._existing_data["transmission_from_instance_to_instance_region"][from_instance][to_instance]
            ):
                has_missing_information = True
                self._existing_data["transmission_from_instance_to_instance_region"][from_instance][to_instance][
                    from_region_str
                ] = {}
            if (
                to_region_str
                not in self._existing_data["transmission_from_instance_to_instance_region"][from_instance][to_instance][
                    from_region_str
                ]
            ):
                has_missing_information = True
                self._existing_data["transmission_from_instance_to_instance_region"][from_instance][to_instance][
                    from_region_str
                ][to_region_str] = 0

            self._existing_data["transmission_from_instance_to_instance_region"][from_instance][to_instance][
                from_region_str
            ][to_region_str] += 1

            if (
                self._existing_data["transmission_from_instance_to_instance_region"][from_instance][to_instance][
                    from_region_str
                ][to_region_str]
                < KEEP_ALIVE_DATA_COUNT
            ):
                has_missing_information = True

        return has_missing_information

    def _format_collected_logs(self) -> list[dict[str, Any]]:
        logs: list[tuple[datetime, dict]] = []
        for workflow_run_sample in self._collected_logs.values():
            if not workflow_run_sample.is_complete():
                continue

            log = workflow_run_sample.to_dict()

            self._extend_existing_execution_instance_region(log[1])
            self._extend_existing_transmission_from_instance_to_instance_region(log[1])

            logs.append(
                log,
            )
        sorted_by_start_time_oldest_first = sorted(logs, key=lambda x: x[0])
        result_logs = [log[1] for log in sorted_by_start_time_oldest_first]
        return result_logs

    def _extend_existing_execution_instance_region(self, log: dict[str, Any]) -> None:
        for function, execution_data in log["execution_data"].items():
            if function not in self._existing_data["execution_instance_region"]:
                self._existing_data["execution_instance_region"][function] = {}
            provider_region = execution_data["provider_region"]
            if provider_region not in self._existing_data["execution_instance_region"][function]:
                self._existing_data["execution_instance_region"][function][provider_region] = 0
            self._existing_data["execution_instance_region"][function][provider_region] += 1

    def _extend_existing_transmission_from_instance_to_instance_region(self, log: dict[str, Any]) -> None:
        for transmission_data in log["direct_transmission_data"]:
            from_instance = transmission_data["from_instance"]
            to_instance = transmission_data["to_instance"]
            from_region_str = (
                transmission_data["from_region"]["provider"] + ":" + transmission_data["from_region"]["region"]
            )
            to_region_str = transmission_data["to_region"]["provider"] + ":" + transmission_data["to_region"]["region"]
            if from_instance not in self._existing_data["transmission_from_instance_to_instance_region"]:
                self._existing_data["transmission_from_instance_to_instance_region"][from_instance] = {}
            if to_instance not in self._existing_data["transmission_from_instance_to_instance_region"][from_instance]:
                self._existing_data["transmission_from_instance_to_instance_region"][from_instance][to_instance] = {}
            if (
                from_region_str
                not in self._existing_data["transmission_from_instance_to_instance_region"][from_instance][to_instance]
            ):
                self._existing_data["transmission_from_instance_to_instance_region"][from_instance][to_instance][
                    from_region_str
                ] = {}
            if (
                to_region_str
                not in self._existing_data["transmission_from_instance_to_instance_region"][from_instance][to_instance][
                    from_region_str
                ]
            ):
                self._existing_data["transmission_from_instance_to_instance_region"][from_instance][to_instance][
                    from_region_str
                ][to_region_str] = 0

            self._existing_data["transmission_from_instance_to_instance_region"][from_instance][to_instance][
                from_region_str
            ][to_region_str] += 1

    def _filter_daily_invocation_counts(self, previous_daily_invocation_counts: dict) -> None:
        oldest_allowed_date = datetime.now(GLOBAL_TIME_ZONE) - timedelta(days=FORGETTING_TIME_DAYS)
        previous_daily_invocation_counts_keys = set(previous_daily_invocation_counts.keys())
        for date_str in previous_daily_invocation_counts_keys:
            date = datetime.strptime(date_str, TIME_FORMAT_DAYS)
            if date < oldest_allowed_date:
                del previous_daily_invocation_counts[date_str]

    def _merge_daily_invocation_counts(self, previous_daily_invocation_counts: dict) -> None:
        for date_str, invocation_set in self._daily_invocation_set.items():
            if date_str not in previous_daily_invocation_counts:
                previous_daily_invocation_counts[date_str] = 0
            previous_daily_invocation_counts[date_str] += len(invocation_set)
