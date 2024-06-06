from datetime import datetime
from typing import Any, Optional


class ExecutionToSuccessorData:  # pylint: disable=too-many-instance-attributes
    def __init__(
        self,
        successor_instance_name: str,
    ) -> None:
        self.successor_instance_name: str = successor_instance_name

        # Task type -> SuccessorTaskType
        ## INVOKE_SUCCESSOR_ONLY = 1
        ## SYNC_UPLOAD_ONLY = 2
        ## SYNC_UPLOAD_AND_INVOKE = 3
        ## CONDITIONALLY_NOT_INVOKE = 4
        self.task_type: Optional[str] = None

        # For SuccessorTaskType 1-4
        self.invocation_time_from_function_start: Optional[float] = None
        self.finish_time_from_function_start: Optional[float] = None

        # Invocation data size (Payload data size)
        # Output data
        self.payload_data_size: Optional[float] = None

        # Upload data size (To DynamoDB or S3)
        # Output data
        self.upload_data_size: Optional[float] = None

        # Upload RTT
        self.upload_rtt: Optional[float] = None

        # Upload total consumed write capacity
        self.consumed_write_capacity: Optional[float] = None

        # Total downloaded data
        # From sync table
        self.sync_data_response_size: Optional[float] = None

        # Destination region of the successor
        self.destination_region: Optional[dict[str, str]] = None

        # Invoking sync node (Sending information)
        # Used for invoking descendent, output data
        self.invoking_sync_node_data_output: dict[str, float] = {}

    def get_total_output_data_size(self) -> float:
        total_upload_data_size = 0.0

        if self.payload_data_size:
            total_upload_data_size += self.payload_data_size

        if self.upload_data_size:
            total_upload_data_size += self.upload_data_size

        for data_size in self.invoking_sync_node_data_output.values():
            total_upload_data_size += data_size

        return total_upload_data_size

    def get_total_input_data_size(self) -> float:
        total_download_data_size = 0.0

        if self.sync_data_response_size:
            total_download_data_size += self.sync_data_response_size

        return total_download_data_size

    def to_dict(self) -> tuple[datetime, dict[str, Any]]:
        # Only return the fields that are not None
        result = {
            "successor_instance_name": self.successor_instance_name,
            "task_type": self.task_type,
            "invocation_time_from_function_start": self.invocation_time_from_function_start,
            # "finish_time_from_function_start": self.finish_time_from_function_start,
            # "invocation_data_size": self.invocation_data_size,
            # "upload_data_size": self.upload_data_size,
            # "upload_rtt": self.upload_rtt,
            # "consumed_write_capacity": self.consumed_write_capacity,
            # "sync_data_response_size": self.sync_data_response_size,
            "destination_region": f"{self.destination_region['provider']}:{self.destination_region['region']}" if self.destination_region else None,
            "invoking_sync_node_data_output": self.invoking_sync_node_data_output,
        }

        # Filter out fields that are None
        filtered_result = {key: value for key, value in result.items() if value is not None and value != {}}

        return filtered_result
