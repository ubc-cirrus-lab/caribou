import json
import re
from datetime import datetime, timedelta
from typing import Any, Optional
import logging

from multi_x_serverless.common.constants import (
    FORGETTING_NUMBER,
    FORGETTING_TIME_DAYS,
    GLOBAL_TIME_ZONE,
    LOG_VERSION,
    TIME_FORMAT,
    WORKFLOW_SUMMARY_TABLE,
    TIME_FORMAT_DAYS
)
from multi_x_serverless.common.models.remote_client.remote_client import RemoteClient
from multi_x_serverless.common.models.remote_client.remote_client_factory import RemoteClientFactory
from multi_x_serverless.syncers.workflow_run_sample import WorkflowRunSample

logger = logging.getLogger(__name__)


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

    def _load_information(self, deployment_manager_config_str: str) -> None:
        deployment_manager_config = json.loads(deployment_manager_config_str)
        deployed_regions_str = deployment_manager_config.get("deployed_regions", "{}")
        self._deployed_regions = json.loads(deployed_regions_str)

    def _get_remote_client(self, provider_region: dict[str, str]) -> RemoteClient:
        if (provider_region["provider"], provider_region["region"]) not in self._region_clients:
            self._region_clients[(provider_region["provider"], provider_region["region"])] = (
                RemoteClientFactory.get_remote_client(provider_region["provider"], provider_region["region"])
            )
        return self._region_clients[(provider_region["provider"], provider_region["region"])]

    def sync_workflow(self) -> None:
        self._sync_logs()
        logger.info(f"Syncing logs for {self.workflow_id} finished")
        data_for_upload: str = self._prepare_data_for_upload(self._previous_data)
        logger.info(f"Uploading data for {self.workflow_id}")
        self._upload_data(data_for_upload)

    def _upload_data(self, data_for_upload: str) -> None:
        self._workflow_summary_client.update_value_in_table(
            WORKFLOW_SUMMARY_TABLE,
            self.workflow_id,
            data_for_upload,
        )

    def _sync_logs(self) -> None:
        for function_physical_instance, instance_information in self._deployed_regions.items():
            logger.info(f"Syncing logs for {function_physical_instance}")
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
        logger.info(
            f"Processing {len(logs)} logs for {functions_instance} in {provider_region} between {time_from} and {time_to}"
        )
        for log in logs:
            self._process_log_entry(log, provider_region)

    def _process_log_entry(self, log_entry: str, provider_region: dict[str, str]) -> None:
        # If this log entry contains an init duration, then this run has incurred a cold start.
        # We taint the sample with this information.
        if "REPORT" in log_entry and "Init Duration" in log_entry:
            request_id = self._extract_from_string(log_entry, r"RequestId: (.*?)\t")
            if request_id:
                self._tainted_cold_start_samples.add(request_id)

        # Ensure that the log entry is a valid log entry and has the correct version
        if f"LOG_VERSION ({LOG_VERSION})" not in log_entry or "[INFO]" not in log_entry:
            return

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

        if len(self._collected_logs) == FORGETTING_NUMBER and not self._forgetting:
            self._check_to_forget()

        if run_id not in self._collected_logs:
            # If we don't need to actually load the log, we can can return here
            # We still need to collect the other logs as they might contain
            # information about the already collected logs
            if self._forgetting or run_id in self._blacklisted_run_ids:
                return
            self._collected_logs[run_id] = WorkflowRunSample(run_id)
        workflow_run_sample = self._collected_logs[run_id]
        workflow_run_sample.update_log_end_time(log_time_dt)

        # Extract the request_id from the log entry
        parts = log_entry.split("\t")
        request_id = parts[2]
        workflow_run_sample.request_ids.add(request_id)

        # Process the log entry
        self._handle_system_log_messages(log_entry, workflow_run_sample, provider_region, log_time_dt)

    def _handle_system_log_messages(
        self,
        log_entry: str,
        workflow_run_sample: WorkflowRunSample,
        provider_region: dict[str, str],
        log_time: datetime,
    ) -> None:
        if "ENTRY_POINT" in log_entry:
            self._extract_entry_point_log(workflow_run_sample, log_entry, provider_region, log_time)
        if "INVOKED" in log_entry:
            self._extract_invoked_logs(workflow_run_sample, log_entry, provider_region, log_time)
        if "EXECUTED" in log_entry:
            self._extract_executed_logs(workflow_run_sample, log_entry)
        if "INVOKING_SUCCESSOR" in log_entry:
            self._extract_invoking_successor_logs(
                workflow_run_sample,
                log_entry,
                provider_region,
                log_time,
            )
        if "CONDITIONAL_NON_EXECUTION" in log_entry:
            self._extract_conditional_non_execution_logs(workflow_run_sample, log_entry)

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
        if len(self._collected_logs) == FORGETTING_NUMBER:
            self._forgetting = True

    def _extract_from_string(self, log_entry: str, regex: str) -> Optional[str]:
        match = re.search(regex, log_entry)
        return match.group(1) if match else None

    def _extract_entry_point_log(
        self,
        workflow_run_sample: WorkflowRunSample,
        log_entry: str,
        provider_region: dict[str, str],
        log_time: datetime,
    ) -> None:
        workflow_run_sample.log_start_time = log_time

        # Extract the start hop latency and data transfer size from the log entry
        data_transfer_size = self._extract_from_string(log_entry, r"PAYLOAD_SIZE \((.*?)\)")
        if data_transfer_size:
            data_transfer_size_fl = float(data_transfer_size)
            workflow_run_sample.start_hop_data_transfer_size = data_transfer_size_fl
        start_hop_latency = self._extract_from_string(log_entry, r"INIT_LATENCY \((.*?)\)")
        if start_hop_latency and start_hop_latency != "N/A":
            start_hop_latency_fl = float(start_hop_latency)
            workflow_run_sample.start_hop_latency = start_hop_latency_fl
        workflow_run_sample.start_hop_destination = provider_region

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

    def _extract_executed_logs(self, workflow_run_sample: WorkflowRunSample, log_entry: str) -> None:
        function_executed = self._extract_from_string(log_entry, r"INSTANCE \((.*?)\)")
        if not isinstance(function_executed, str):
            raise ValueError(f"Invalid function_executed: {function_executed}")
        duration = self._extract_from_string(log_entry, r"EXECUTION_TIME \((.*?)\)")
        if duration:
            duration = float(duration)  # type: ignore
        if not isinstance(duration, float):
            raise ValueError(f"Invalid duration: {duration}")

        workflow_run_sample.execution_latencies[function_executed] = duration

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
            transmission_size = float(data_transfer_size)
        if not isinstance(transmission_size, float):
            raise ValueError(f"Invalid data_transfer_size: {data_transfer_size}")

        transmission_data = workflow_run_sample.get_transmission_data(taint)
        transmission_data.from_region = provider_region
        transmission_data.from_instance = caller_function
        transmission_data.to_instance = callee_function
        transmission_data.transmission_start_time = log_time
        transmission_data.transmission_size = transmission_size

    def _extract_conditional_non_execution_logs(self, workflow_run_sample: WorkflowRunSample, log_entry: str) -> None:
        caller_function = self._extract_from_string(log_entry, r"INSTANCE \((.*?)\)")
        if not isinstance(caller_function, str):
            raise ValueError(f"Invalid caller_function: {caller_function}")

        callee_function = self._extract_from_string(log_entry, r"SUCCESSOR \((.*?)\)")
        if not isinstance(callee_function, str):
            raise ValueError(f"Invalid callee_function: {callee_function}")
        
        if caller_function not in workflow_run_sample._non_executions:
            workflow_run_sample._non_executions[caller_function] = {}
        if callee_function not in workflow_run_sample._non_executions[caller_function]:
            workflow_run_sample._non_executions[caller_function][callee_function] = 0
        workflow_run_sample._non_executions[caller_function][callee_function] += 1

    def _prepare_data_for_upload(self, previous_data: dict) -> str:
        previous_daily_invocation_counts = previous_data.get("daily_invocation_counts", {})

        self._filter_daily_invocation_counts(previous_daily_invocation_counts)
        self._merge_daily_invocation_counts(previous_daily_invocation_counts)

        daily_invocation_counts = previous_daily_invocation_counts

        collected_logs: list[dict[str, Any]] = self._format_collected_logs()
        if len(collected_logs) < FORGETTING_NUMBER:
            self._fill_up_collected_logs(collected_logs, previous_data)

        workflow_runtime_samples: list[float] = [collected_logs["runtime"] for collected_logs in collected_logs]

        data_to_upload = {
            "daily_invocation_counts": daily_invocation_counts,
            "logs": collected_logs,
            "workflow_runtime_samples": workflow_runtime_samples,
            "last_sync_time": self._time_intervals_to_sync[-1][1].strftime(TIME_FORMAT),
        }

        return json.dumps(data_to_upload)

    def _fill_up_collected_logs(self, collected_logs: list[dict[str, Any]], previous_data: dict) -> None:
        oldest_allowed_date = datetime.now(GLOBAL_TIME_ZONE) - timedelta(days=FORGETTING_TIME_DAYS)
        previous_logs = previous_data.get("logs", [])
        # Iterate over the previous logs in reverse order,
        for previous_log in reversed(previous_logs):
            # Do this until we either exceed the forgetting number or run out of previous logs
            if len(collected_logs) >= FORGETTING_NUMBER:
                break
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

    def _format_collected_logs(self) -> list[dict[str, Any]]:
        logs: list[tuple[datetime, dict]] = []
        for workflow_run_sample in self._collected_logs.values():
            if not workflow_run_sample.is_complete():
                continue
            logs.append(
                workflow_run_sample.to_dict(),
            )
        sorted_by_start_time_oldest_first = sorted(logs, key=lambda x: x[0])
        result_logs = [log[1] for log in sorted_by_start_time_oldest_first]
        return result_logs

    def _filter_daily_invocation_counts(
        self, previous_daily_invocation_counts: dict
    ) -> None:
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
