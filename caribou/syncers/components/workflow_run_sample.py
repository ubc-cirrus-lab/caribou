from datetime import datetime, timedelta
from typing import Any, Optional

from caribou.common.constants import TIME_FORMAT
from caribou.syncers.components.execution_data import ExecutionData
from caribou.syncers.components.transmission_data import TransmissionData


class WorkflowRunSample:  # pylint: disable=too-many-instance-attributes
    def __init__(self, run_id: str) -> None:
        self.run_id: str = run_id
        self.request_ids: set[str] = set()

        # Used for ensuring that we only observe workflow samples
        # that does not have duplicate invocations in the same run.
        self.encountered_instance_request_ids: dict[str, set[str]] = {}

        self.log_start_time: Optional[datetime] = None
        self.log_end_time: Optional[datetime] = None

        # Execution and transmission data/information
        self.execution_data: dict[str, ExecutionData] = {}
        self.transmission_data: dict[str, TransmissionData] = {}

        # Start hop informations
        self.start_hop_latency: float = 0.0
        self.start_hop_data_transfer_size: float = 0.0
        self.start_hop_instance_name: Optional[str] = None
        self.start_hop_destination: Optional[str] = None
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

    def get_execution_data(self, instance_name: str, request_id: Optional[str]) -> ExecutionData:
        # Add the request ID to the encountered request IDs
        # This is to blacklist runs that have duplicate invocations
        # As they distort the data.
        if request_id is not None:
            if instance_name not in self.encountered_instance_request_ids:
                self.encountered_instance_request_ids[instance_name] = set()
            self.encountered_instance_request_ids[instance_name].add(request_id)

        if instance_name not in self.execution_data:
            self.execution_data[instance_name] = ExecutionData(instance_name)
        return self.execution_data[instance_name]

    def is_valid_and_complete(self) -> bool:
        # Check if the log start and end time is set
        # Also check if there are no duplicate instances
        # Also check if ALL execution data is completed
        return (
            self.log_start_time is not None
            and self.log_end_time is not None
            and not self._has_duplicate_instances()
            and not self._has_incomplete_execution_data()
        )

    def _has_duplicate_instances(self) -> bool:
        for instance_request_ids in self.encountered_instance_request_ids.values():
            if len(instance_request_ids) > 1:
                return True
        return False

    def _has_incomplete_execution_data(self) -> bool:
        for execution_data in self.execution_data.values():
            if not execution_data.is_completed:
                return True
        return False

    def _get_formatted_execution_data(self) -> list[dict[str, Any]]:
        formatted_execution_data = []
        for execution_data in self.execution_data.values():
            if execution_data.is_completed:
                formatted_execution_data.append(execution_data.to_dict())
        return formatted_execution_data

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
                "runtime_s": self.duration.total_seconds(),
                "execution_data": self._get_formatted_execution_data(),
                "transmission_data": self._get_formatted_invocation_transmission_data(),
                "start_hop_info": {
                    "destination": self.start_hop_destination,
                    "data_transfer_size_gb": self.start_hop_data_transfer_size,
                    "latency_s": self.start_hop_latency,
                    "workflow_placement_decision": {
                        "data_size_gb": self.start_hop_wpd_data_size,
                        "consumed_read_capacity": self.start_hop_wpd_consumed_read_capacity,
                    },
                },
                "unique_cpu_models": list(self.cpu_models),
            },
        )
