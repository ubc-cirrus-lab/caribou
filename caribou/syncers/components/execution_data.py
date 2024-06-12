from typing import Any, Optional

from caribou.syncers.components.execution_to_successor_data import ExecutionToSuccessorData


class ExecutionData:  # pylint: disable=too-many-instance-attributes
    def __init__(self, instance_name: str):
        self.instance_name: str = instance_name

        self.is_entry_point: bool = False

        # Start hop input size (data input)
        self.start_hop_payload_size: Optional[float] = None

        self.request_id: Optional[str] = None
        self.cpu_model: Optional[str] = None

        # Captured for user code execution
        self.user_execution_duration: Optional[float] = None

        # Captured after waiting for all async invocations are completed
        self.execution_duration: Optional[float] = None

        # This is the reported duration from the Lambda logs (Should be longest)
        self.reported_duration: Optional[float] = None

        # This is where the instance was executed
        self.provider_region: Optional[dict[str, str]] = None

        # Download data size (From DynamoDB or S3)
        self.download_size: Optional[float] = None

        # Download time
        self.download_time: Optional[float] = None

        # Consumed read capacity
        self.consumed_read_capacity: Optional[float] = None

        # This is information from lambda insights
        # Used for CPU utilization and also all IO calculations
        self.lambda_insights: Optional[dict[str, Any]] = None

        # This is regarding when it calls the next instance
        # Records when the next instance executes and also
        # All data regarding the transmission.
        self.successor_data: dict[str, ExecutionToSuccessorData] = {}

    def get_successor_data(self, successor_instance_name: str) -> ExecutionToSuccessorData:
        if successor_instance_name not in self.successor_data:
            self.successor_data[successor_instance_name] = ExecutionToSuccessorData(successor_instance_name)
        return self.successor_data[successor_instance_name]

    def _get_total_output_data_size(self) -> float:
        total_output_data_size = 0.0

        for successor_data in self.successor_data.values():
            total_output_data_size += successor_data.get_total_output_data_size()

        return total_output_data_size

    def _get_total_input_data_size(self) -> float:
        total_input_data_size = 0.0

        if self.start_hop_payload_size:
            total_input_data_size += self.start_hop_payload_size

        if self.download_size:
            total_input_data_size += self.download_size

        for successor_data in self.successor_data.values():
            total_input_data_size += successor_data.get_total_input_data_size()

        return total_input_data_size

    @property
    def input_data_transfer_during_execution(self) -> float:
        if self.lambda_insights is None:
            return 0.0

        insights_total_recieved_data = self.lambda_insights.get("rx_bytes", 0.0) / (1024**3)  # Convert bytes to GB
        other_recieved_data = max(0.0, insights_total_recieved_data - self._get_total_input_data_size())

        return other_recieved_data

    @property
    def output_data_transfer_during_execution(self) -> float:
        if self.lambda_insights is None:
            return 0.0

        insights_total_send_data = self.lambda_insights.get("tx_bytes", 0.0) / (1024**3)  # Convert bytes to GB
        other_send_data = max(0.0, insights_total_send_data - self._get_total_output_data_size())

        return other_send_data

    @property
    def data_transfer_during_execution(self) -> float:
        return self.input_data_transfer_during_execution + self.output_data_transfer_during_execution

    @property
    def longest_duration(self) -> float:
        # Max of all non-None durations
        # First get a list of all non-None durations
        lambda_insight_duration = None
        if self.lambda_insights is not None:
            lambda_insight_duration = self.lambda_insights.get("duration", None)

        durations = [
            duration
            for duration in [
                self.user_execution_duration,
                self.execution_duration,
                self.reported_duration,
                lambda_insight_duration,
            ]
            if duration is not None
        ]

        # If there are no durations, return 0
        if not durations:
            return 0.0

        return max(durations)

    @property
    def cpu_utilization(self) -> Optional[float]:
        if self.lambda_insights is None:
            return None

        cpu_total_time = self.lambda_insights.get("cpu_total_time", None)
        if cpu_total_time is None:
            return None

        # In megabytes
        total_memory = self.lambda_insights.get("total_memory", 0.0)

        # vcpu ratio (assuming linear, intercept at 0 scaling)
        # for aws lambda https://docs.aws.amazon.com/lambda/latest/dg/configuration-memory.html
        vcpu = total_memory / 1769

        # Calculate the cpu utilization
        cpu_utilization = cpu_total_time / (self.longest_duration * vcpu)

        return cpu_utilization

    @property
    def is_completed(self) -> bool:
        # Only the relevant entries
        return all(
            [
                self.instance_name,
                self.request_id,
                self.lambda_insights,
            ]
        )

    def to_dict(self) -> dict[str, Any]:
        download_information: Optional[dict[str, Any]] = None
        if self.download_size is not None:
            download_information = {
                "download_size": self.download_size,
                "download_time": self.download_time,
                "consumed_read_capacity": self.consumed_read_capacity,
            }

        relevant_insights: Optional[dict[str, Any]] = None
        if self.lambda_insights is not None:
            relevant_insights = {
                "cpu_total_time": self.lambda_insights.get("cpu_total_time", None),
                "duration": self.lambda_insights.get("duration", None),
                "total_memory": self.lambda_insights.get("total_memory", None),
            }

        # Only return the fields that are not None
        result = {
            "instance_name": self.instance_name,
            # "is_entry_point": self.is_entry_point,
            # "request_id": self.request_id,
            "user_code_duration": self.user_execution_duration,
            "duration": self.longest_duration,
            "cpu_model": self.cpu_model,
            # "execution_duration": self.execution_duration,
            # "reported_duration": self.reported_duration,
            "provider_region": self._format_region(self.provider_region),
            "download_information": download_information,
            # "other_recieved_data": self.other_recieved_data, # Can be commented out
            # "other_send_data": self.other_send_data, # Can be commented out
            "data_transfer_during_execution": self.data_transfer_during_execution,
            # "total_consumed_write_capacity": self.total_consumed_write_capacity,
            "cpu_utilization": self.cpu_utilization,
            # "lambda_insights": self.lambda_insights,
            "relevant_insights": relevant_insights,
            "successor_data": {k: v.to_dict() for k, v in self.successor_data.items()},  # Can be commented out
        }

        # Filter out fields that are None
        filtered_result = {key: value for key, value in result.items() if value is not None and value != {}}

        return filtered_result

    def _format_region(self, region: Optional[dict[str, str]]) -> Optional[str]:
        if region:
            return f"{region['provider']}:{region['region']}"
        return None
