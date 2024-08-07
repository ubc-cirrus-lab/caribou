from datetime import datetime
from typing import Any, Optional


class TransmissionData:  # pylint: disable=too-many-instance-attributes
    def __init__(self, taint: str):
        self.taint = taint
        self.transmission_start_time: Optional[datetime] = None
        self.transmission_end_time: Optional[datetime] = None
        self.payload_transmission_size: float = 0.0
        self.from_instance: Optional[str] = None
        self.to_instance: Optional[str] = None
        self.from_region: Optional[str] = None
        self.to_region: Optional[str] = None

        # Info regarding if the successor was invoked
        self.successor_invoked: Optional[bool] = None

        # Denotes if it contains sync information
        # This is only for sync node ancestors
        self.contains_sync_information: bool = False

        # For sync nodes
        self.upload_size: Optional[float] = None

        # RTT for uploading to dynamoDB
        # Can be used for future analysis
        self.upload_rtt: Optional[float] = None

        self.consumed_write_capacity: Optional[float] = None
        self.sync_data_response_size: Optional[float] = None
        self.from_direct_successor: Optional[bool] = None
        self.redirector_transmission: Optional[bool] = None  # TODO: Examine if we should remove or keep this

        # If not from a direct successor,
        # this will significantly affect what we want to collect
        # Here we have (starting instance -> uninvoked successor instance,
        # simulated sync predecessor -> sync node)
        # Where the from_instance is the starting instance, the
        # to_instance is the sync node. Now we also want
        # to know the uninvoked successor instance, and the
        # simulated sync predecessor.
        self.uninvoked_instance: Optional[str] = None
        self.simulated_sync_predecessor: Optional[str] = None

    def to_dict(self) -> dict[str, Any]:
        sync_information: Optional[dict[str, Any]] = None
        if self.contains_sync_information:
            sync_information = {
                "upload_size_gb": self.upload_size,
                "consumed_write_capacity": self.consumed_write_capacity,
                "sync_data_response_size_gb": self.sync_data_response_size,
            }

            # Filter out fields that are None
            sync_information = {key: value for key, value in sync_information.items() if value is not None}

        # Only return the fields that are not None
        result = {
            "transmission_size_gb": self.payload_transmission_size,
            "transmission_latency_s": self.transmission_latency,
            "from_instance": self.from_instance,
            "uninvoked_instance": self.uninvoked_instance,
            "simulated_sync_predecessor": self.simulated_sync_predecessor,
            "to_instance": self.to_instance,
            "from_region": self.from_region,
            "to_region": self.to_region,
            "successor_invoked": self.successor_invoked,
            "from_direct_successor": self.from_direct_successor,
            "redirector_transmission": self.redirector_transmission,
            "sync_information": sync_information,
        }

        # Filter out fields that are None
        filtered_result = {key: value for key, value in result.items() if value is not None}

        return filtered_result

    @property
    def transmission_latency(self) -> Optional[float]:
        if not self.transmission_start_time or not self.transmission_end_time:
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
