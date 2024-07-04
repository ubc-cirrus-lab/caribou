from typing import Any, Optional


class ExecutionToSuccessorData:  # pylint: disable=too-many-instance-attributes
    def __init__(
        self,
        successor_instance_name: str,
    ) -> None:
        self.successor_instance_name: str = successor_instance_name

        # Task type
        self.task_type: Optional[str] = None

        # May be used for future analysis
        self.invocation_time_from_function_start: Optional[float] = None
        self.finish_time_from_function_start: Optional[float] = None

        # Invocation data size (Payload data size)
        # Output data
        self.payload_data_size: Optional[float] = None

        # Upload data size (To DynamoDB or S3)
        # Output data
        self.upload_data_size: Optional[float] = None

        # Upload total consumed write capacity
        self.consumed_write_capacity: Optional[float] = None

        # Total downloaded data
        # From sync table
        self.sync_data_response_size: Optional[float] = None

        # Destination region of the successor
        # This is not accurate for non-execution nodes
        # Due to the multiple sync updates
        self.destination_region: Optional[str] = None

        # Invoking sync node (Sending information)
        # Used for invoking descendent, output data
        self.invoking_sync_node_data_output: dict[str, dict[str, Any]] = {}

    def get_total_output_data_size(self) -> float:
        total_upload_data_size = 0.0

        if self.payload_data_size:
            total_upload_data_size += self.payload_data_size

        if self.upload_data_size:
            total_upload_data_size += self.upload_data_size

        for data_size_info in self.invoking_sync_node_data_output.values():
            data_size: float = data_size_info.get("data_transfer_size", 0.0)
            total_upload_data_size += data_size

        return total_upload_data_size

    def get_total_input_data_size(self) -> float:
        total_download_data_size = 0.0

        if self.sync_data_response_size:
            total_download_data_size += self.sync_data_response_size

        return total_download_data_size

    def to_dict(self) -> dict[str, Any]:
        # Only return the fields that are not None
        result = {
            "task_type": self.task_type,
            "invocation_time_from_function_start_s": self.invocation_time_from_function_start,
            "sync_info": self.invoking_sync_node_data_output if self.invoking_sync_node_data_output else None,
            "consumed_write_capacity": self.consumed_write_capacity if self.consumed_write_capacity else None,
        }

        # Filter out fields that are None
        filtered_result = {key: value for key, value in result.items() if value is not None}

        return filtered_result
