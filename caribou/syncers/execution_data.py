from datetime import datetime, timedelta
from typing import Any, Optional


class ExecutionData:  # pylint: disable=too-many-instance-attributes
    def __init__(self, instance_name: str):
        self.instance_name: str = instance_name

        # These are the attributes that must be present in the ExecutionData class
        self.instance_name: Optional[str] = None
        self.request_id: Optional[str] = None
        self.cpu_model: Optional[str] = None

        # This is the our captured duration from our logs
        self.execution_duration: Optional[float] = None

        # This is the reported duration from the Lambda logs
        self.reported_duration: Optional[float] = None

        # This is where the instance was executed
        self.provider_region: Optional[dict[str, str]] = None

        # This is when the instance invokes its successor
        self.successor_invocation_time: Optional[datetime] = None

        # This is regarding the sync node payload
        # and predecessor counter tables, and all transmission outputs of the node.

        # This is information from lambda insights
        self.insights: Optional[dict[str, Any]] = None

    # def to_dict(self) -> dict[str, Any]:
    #     return {
    #         "transmission_size": self.transmission_size,
    #         "transmission_latency": self.transmission_latency.total_seconds(),
    #         "from_direct_successor": self.from_direct_successor,
    #         "from_instance": self.from_instance,
    #         "to_instance": self.to_instance,
    #         "from_region": self.from_region,
    #         "to_region": self.to_region,
    #     }

    # @property
    # def transmission_latency(self) -> timedelta:
    #     if not self.transmission_start_time or not self.transmission_end_time:
    #         raise ValueError(
    #             "transmission_start_time or transmission_end_time is not set, "
    #             "this should not happen, was is_completed called?"
    #         )
    #     return self.transmission_end_time - self.transmission_start_time

    # @property
    # def is_completed(self) -> bool:
    #     return all(
    #         [
    #             self.transmission_start_time,
    #             self.transmission_end_time,
    #             self.from_instance,
    #             self.to_instance,
    #             self.from_region,
    #             self.to_region,
    #         ]
    #     )

    # def __str__(self) -> str:
    #     return f"OrchestrationTransmissionData({self.taint}, {self.transmission_start_time}, {self.transmission_end_time}, {self.from_direct_successor}, {self.transmission_size}, {self.from_instance}, {self.to_instance}, {self.from_region}, {self.to_region})"  # pylint: disable=line-too-long
