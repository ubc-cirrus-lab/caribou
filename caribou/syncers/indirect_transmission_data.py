from datetime import datetime
from typing import Any


class IndirectTransmissionData:  # pylint: disable=too-many-instance-attributes
    def __init__(
        self,
        transmission_start_time: datetime,
        upload_rtt: float,
        potential_transmission_size: float,
        from_instance: str,
        to_instance: str,
        from_region: dict[str, str],
        to_region: dict[str, str],
    ):
        self.transmission_start_time: datetime = transmission_start_time
        self.upload_rtt: float = upload_rtt
        self.potential_transmission_size: float = potential_transmission_size
        self.from_instance: str = from_instance
        self.to_instance: str = to_instance
        self.from_region: dict[str, str] = from_region
        self.to_region: dict[str, str] = to_region

    def to_dict(self) -> dict[str, Any]:
        return {
            "upload_rtt": self.upload_rtt,
            "potential_transmission_size": self.potential_transmission_size,
            "from_instance": self.from_instance,
            "to_instance": self.to_instance,
            "from_region": self.from_region,
            "to_region": self.to_region,
        }

    def __str__(self) -> str:
        return f"IndirectTransmissionData({self.transmission_start_time}, {self.upload_rtt}, {self.potential_transmission_size}, {self.from_instance}, {self.to_instance}, {self.from_region}, {self.to_region})"  # pylint: disable=line-too-long
