import logging
from datetime import datetime, timedelta
from typing import Any, Optional

logger = logging.getLogger(__name__)


class TransmissionData:  # pylint: disable=too-many-instance-attributes
    def __init__(self, taint: str):
        self.taint = taint
        self.transmission_start_time: Optional[datetime] = None
        self.transmission_end_time: Optional[datetime] = None
        self.transmission_size: float = 0.0
        self.from_instance: Optional[str] = None
        self.to_instance: Optional[str] = None
        self.from_region: Optional[dict[str, str]] = None
        self.to_region: Optional[dict[str, str]] = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "transmission_size": self.transmission_size,
            "transmission_latency": self.transmission_latency.total_seconds(),
            "from_instance": self.from_instance,
            "to_instance": self.to_instance,
            "from_region": self.from_region,
            "to_region": self.to_region,
        }

    @property
    def transmission_latency(self) -> timedelta:
        if not self.transmission_start_time or not self.transmission_end_time:
            raise ValueError(
                "transmission_start_time or transmission_end_time is not set, "
                "this should not happen, was is_completed called?"
            )
        return self.transmission_end_time - self.transmission_start_time

    @property
    def is_completed(self) -> bool:
        return all(
            [
                self.transmission_start_time,
                self.transmission_end_time,
                self.from_instance,
                self.to_instance,
                self.from_region,
                self.to_region,
            ]
        )

    def __str__(self) -> str:
        return f"TransmissionData({self.taint}, {self.transmission_start_time}, {self.transmission_end_time}, {self.transmission_size}, {self.from_instance}, {self.to_instance}, {self.from_region}, {self.to_region})"  # pylint: disable=line-too-long
