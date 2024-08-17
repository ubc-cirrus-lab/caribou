from datetime import datetime, timedelta
from typing import Any, Optional

from caribou.common.constants import TIME_FORMAT
from caribou.syncers.components.execution_data import ExecutionData
from caribou.syncers.components.start_hop_data import StartHopData
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
        self.start_hop_data: StartHopData = StartHopData()

        # Encountered CPUs in the run.
        self.cpu_models: set[str] = set()

        # Flag to indicate if the WPD size has already been attributed to
        # the appropriate instance. This is to prevent double counting.
        self._already_attributed_wpd_size: bool = False

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

        # Set the request ID if it is not set
        if self.execution_data[instance_name].request_id is None:
            self.execution_data[instance_name].request_id = request_id

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
            and self.start_hop_data.is_completed
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

        if not self._already_attributed_wpd_size:
            self._attribute_wpd_size()

        return (
            self.log_start_time,
            {
                "run_id": self.run_id,
                "start_time": self.log_start_time.strftime(TIME_FORMAT),
                "runtime_s": self.duration.total_seconds(),
                "execution_data": self._get_formatted_execution_data(),
                "transmission_data": self._get_formatted_invocation_transmission_data(),
                "start_hop_info": self.start_hop_data.to_dict(),
                "unique_cpu_models": list(self.cpu_models),
            },
        )

    def _attribute_wpd_size(self) -> None:
        self._already_attributed_wpd_size = True  # Flag to prevent double counting

        retrieved_wpd_at_function = self.start_hop_data.retrieved_wpd_at_function
        if not retrieved_wpd_at_function:
            # No WPD size to attribute,
            # it was retrieved at the client
            return

        # Now we need to see if this a redirector exists, if it does
        # We need to attribute the WPD size to the redirector
        redirector_execution_data = self.start_hop_data.redirector_execution_data
        if redirector_execution_data:
            redirector_execution_data.downloaded_wpd_data_size = self.start_hop_data.wpd_data_size
        else:
            # No redirector, we attribute the WPD size to the start hop instance
            start_hop_instance_name = self.start_hop_data.start_hop_instance_name
            if start_hop_instance_name is None:
                raise ValueError("start_hop_instance_name is not set")
            start_hop_execution_data = self.get_execution_data(start_hop_instance_name, None)
            start_hop_execution_data.downloaded_wpd_data_size = self.start_hop_data.wpd_data_size
