from datetime import datetime, timedelta
from typing import Any, Optional

from multi_x_serverless.common.constants import TIME_FORMAT
from multi_x_serverless.syncers.transmission_data import TransmissionData


class WorkflowRunSample:  # pylint: disable=too-many-instance-attributes
    def __init__(self, run_id: str) -> None:
        self.run_id: str = run_id
        self.request_ids: set[str] = set()
        self.log_start_time: Optional[datetime] = None
        self.log_end_time: Optional[datetime] = None
        self.execution_latencies: dict[str, dict[str, Any]] = {}
        self.transmission_data: dict[str, TransmissionData] = {}
        self.start_hop_latency: float = 0.0
        self.start_hop_data_transfer_size: float = 0.0
        self.start_hop_destination: Optional[dict[str, str]] = None
        self.non_executions: dict[str, dict[str, int]] = {}

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

    def is_complete(self) -> bool:
        return self.log_start_time is not None and self.log_end_time is not None

    def _get_formatted_transmission_data(self) -> list[dict[str, Any]]:
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
                "runtime": self.duration.total_seconds(),
                "start_time": self.log_start_time.strftime(TIME_FORMAT),
                "execution_latencies": self.execution_latencies,
                "transmission_data": self._get_formatted_transmission_data(),
                "start_hop_latency": self.start_hop_latency,
                "start_hop_data_transfer_size": self.start_hop_data_transfer_size,
                "start_hop_destination": self.start_hop_destination,
                "non_executions": self.non_executions,
            },
        )
