from datetime import datetime, timedelta
from typing import Any, Optional

from caribou.common.constants import TIME_FORMAT
from caribou.syncers.execution_data import ExecutionData
from caribou.syncers.transmission_data import TransmissionData


class WorkflowRunSample:  # pylint: disable=too-many-instance-attributes
    def __init__(self, run_id: str) -> None:
        self.run_id: str = run_id
        self.request_ids: set[str] = set()

        # Used for ensuring that we only observe workflow samples
        # that does not have duplicate invocations in the same run.
        self.encountered_instance_request_IDs: dict[str, set[str]] = {}

        self.log_start_time: Optional[datetime] = None
        self.log_end_time: Optional[datetime] = None

        # Execution, transmission and non-execution data/information
        self.execution_data: dict[str, ExecutionData] = {}
        self.transmission_data: dict[str, TransmissionData] = {}
        self.non_executions: dict[str, dict[str, int]] = {}

        # Start hop informations
        self.start_hop_latency: float = 0.0
        self.start_hop_data_transfer_size: float = 0.0
        self.start_hop_instance_name: Optional[str] = None
        self.start_hop_destination: Optional[dict[str, str]] = None
        self.start_hop_wpd_data_size: Optional[float] = None
        self.start_hop_wpd_consumed_read_capacity: Optional[float] = None

        # Encountered CPUs in the run.
        self.cpu_models: set[str] = set()

    @property
    def duration(self) -> timedelta:
        if not self.log_end_time or not self.log_start_time:
            raise ValueError(
                "log_end_time or log_start_time is not set, this should not happen, was is_complete called?"
            )
        return self.log_end_time - self.log_start_time

    def update_log_end_time(self, log_end_time: datetime) -> None:
        if self.log_end_time is None or log_end_time > self.log_end_time:
            self.log_end_time = log_end_time

    def get_transmission_data(self, taint: str) -> TransmissionData:
        if taint not in self.transmission_data:
            self.transmission_data[taint] = TransmissionData(taint)
        return self.transmission_data[taint]

    def get_execution_data(self, instance_name: str) -> ExecutionData:
        if instance_name not in self.execution_data:
            self.execution_data[instance_name] = ExecutionData(instance_name)
        return self.execution_data[instance_name]

    def has_duplicate_instances(self) -> bool:
        for instance_request_IDs in self.encountered_instance_request_IDs.values():
            if len(instance_request_IDs) > 1:
                return True
        return False

    def is_valid_and_complete(self) -> bool:
        return self.log_start_time is not None and self.log_end_time is not None and not self.has_duplicate_instances()

    def _get_formatted_invocation_transmission_data(self) -> list[dict[str, Any]]:
        formatted_transmission_data = []
        for transmission_data in self.transmission_data.values():
            if transmission_data.is_completed:
                formatted_transmission_data.append(transmission_data.to_dict())
        return formatted_transmission_data

    def to_dict(self) -> tuple[datetime, dict[str, Any]]:
        if not self.log_start_time:
            raise ValueError("log_start_time is not set")

        return (
            self.log_start_time,
            {
                "run_id": self.run_id,
                "start_time": self.log_start_time.strftime(TIME_FORMAT),
                "runtime": self.duration.total_seconds(),
                "execution_data": self.execution_data,
                "transmission_data": self._get_formatted_invocation_transmission_data(),
                "non_executions": self.non_executions,
                "start_hop_info": {
                    "instance_name": self.start_hop_instance_name,
                    "destination": self.start_hop_destination,
                    "data_transfer_size": self.start_hop_data_transfer_size,
                    "latency": self.start_hop_latency,
                    "workflow_placement_decision": {
                        "data_size": self.start_hop_wpd_data_size,
                        "consumed_read_capacity": self.start_hop_wpd_consumed_read_capacity,
                    },
                },
                "unique_cpu_models": list(self.cpu_models),
            },
        )
