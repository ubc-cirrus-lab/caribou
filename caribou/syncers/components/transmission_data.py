from datetime import datetime
from typing import Any, Optional


class TransmissionData:  # pylint: disable=too-many-instance-attributes
    def __init__(self, taint: str):
        self.taint = taint
        self.transmission_start_time: Optional[datetime] = None
        self.transmission_end_time: Optional[datetime] = None
        self.payload_transmission_size: float = 0.0
        self.successor_invoked: Optional[bool] = None
        self.from_instance: Optional[str] = None
        self.to_instance: Optional[str] = None
        self.from_region: Optional[dict[str, str]] = None
        self.to_region: Optional[dict[str, str]] = None

        # Denots if the time includes a upload time
        # This is only for sync node successors
        self.contains_upload_time: bool = False

        self.upload_size: Optional[float] = None
        self.upload_rtt: Optional[float] = None
        self.consumed_write_capacity: Optional[float] = None
        self.sync_data_response_size: Optional[float] = None

        self.from_direct_successor: Optional[bool] = None

        # If not from a direct successor,
        # This is a proxy call for a successor instance
        self.uninvoked_instance: Optional[str] = None

    def to_dict(self) -> dict[str, Any]:
        sync_information: Optional[dict[str, Any]] = None
        if self.contains_upload_time:
            sync_information = {
                "upload_size": self.upload_size,
                # "upload_rtt": self.upload_rtt,
                "consumed_write_capacity": self.consumed_write_capacity,
                "sync_data_response_size": self.sync_data_response_size,
            }

        # Only return the fields that are not None
        result = {
            "transmission_size": self.payload_transmission_size,
            "transmission_latency": self.transmission_latency,
            "from_instance": self.from_instance,
            "uninvoked_instance": self.uninvoked_instance,
            "to_instance": self.to_instance,
            "from_region": self._format_region(self.from_region),
            "to_region": self._format_region(self.to_region),
            # Info regarding if the successor was invoked
            "successor_invoked": self.successor_invoked,
            # For sync nodes with Conditional calls
            "from_direct_successor": self.from_direct_successor,
            # For sync nodes -> require upload data
            # "contains_upload_time": self.contains_upload_time,
            "sync_information": sync_information,
        }

        # Filter out fields that are None
        filtered_result = {key: value for key, value in result.items() if value is not None}

        return filtered_result

    @property
    def transmission_latency(self) -> Optional[float]:
        if not self.transmission_start_time or not self.transmission_end_time:
            # TODO: Bring back
            # raise ValueError(
            #     "transmission_start_time or transmission_end_time is not set, "
            #     "this should not happen, was is_completed called?"
            # )
            return None
        return (self.transmission_end_time - self.transmission_start_time).total_seconds()

    @property
    def is_completed(self) -> bool:
        return all(
            [
                self.from_instance,
                self.to_instance,
                self.from_region,
                self.to_region,
            ]
        )

        # # return all(
        # #     [
        # #         self.transmission_start_time,
        # #         self.transmission_end_time,
        # #         self.from_instance,
        # #         self.to_instance,
        # #         self.from_region,
        # #         self.to_region,
        # #     ]
        # # )
        # return True

        # # For sync nodes (or Conditional calls)
        # "contains_upload_time": self.contains_upload_time,
        # "upload_size": self.upload_size,
        # "consumed_write_capacity": self.consumed_write_capacity,
        # "sync_data_response_size": self.sync_data_response_size,
        # "from_direct_successor": self.from_direct_successor,
        # "proxy_for_instance": self.proxy_for_instance,

    def _format_region(self, region: Optional[dict[str, str]]) -> Optional[str]:
        if region:
            return f"{region['provider']}:{region['region']}"
        return None

    def __str__(self) -> str:
        # return f"InvocationTransmissionData({self.taint}, {self.transmission_start_time}, {self.transmission_end_time}, {self.from_direct_successor}, {self.transmission_size}, {self.from_instance}, {self.to_instance}, {self.from_region}, {self.to_region})"  # pylint: disable=line-too-long
        return (
            f"InvocationTransmissionData({self.taint}, {self.transmission_start_time}, "
            f"{self.transmission_end_time}, {self.from_direct_successor}, {self.payload_transmission_size}, "
            f"{self.from_instance}, {self.to_instance}, {self.from_region}, {self.to_region}, {self.contains_upload_time}, "
            f"{self.upload_size}, {self.upload_rtt}, {self.consumed_write_capacity}, "
            f"{self.sync_data_response_size}, {self.from_direct_successor}, {self.uninvoked_instance})"
        )
