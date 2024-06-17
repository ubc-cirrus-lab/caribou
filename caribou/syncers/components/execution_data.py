from typing import Any, Optional

from caribou.syncers.components.execution_to_successor_data import ExecutionToSuccessorData


class ExecutionData:  # pylint: disable=too-many-instance-attributes
    def __init__(self, instance_name: str):
        self.instance_name: str = instance_name

        # Input payload size, can also be for
        # Start hop input size (data input)
        self.input_payload_size: float = 0.0

        self.request_id: Optional[str] = None
        self.cpu_model: Optional[str] = None

        # Captured for user code execution
        # May be used for future analysis
        self.user_execution_duration: Optional[float] = None

        # Captured after waiting for all async invocations are completed
        # May be used for future analysis
        self.execution_duration: Optional[float] = None

        # This is where the instance was executed
        self.provider_region: Optional[str] = None

        # Download data size (From DynamoDB or S3)
        # May be used for future analysis
        self.download_size: Optional[float] = None

        # Download time
        # May be used for future analysis
        self.download_time: Optional[float] = None

        # Consumed read capacity
        # May be used for future analysis
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

        if self.input_payload_size:
            total_input_data_size += self.input_payload_size

        if self.download_size:
            total_input_data_size += self.download_size

        for successor_data in self.successor_data.values():
            total_input_data_size += successor_data.get_total_input_data_size()

        return total_input_data_size

    @property
    def data_transfer_during_execution(self) -> float:
        if self.lambda_insights is None:
            return 0.0

        insights_total_network_transfer = self.lambda_insights.get("total_network", 0.0) / (
            1024**3
        )  # Convert bytes to GB
        data_transfer_during_execution = max(
            0.0,
            insights_total_network_transfer - self._get_total_input_data_size() - self._get_total_output_data_size(),
        )

        return data_transfer_during_execution

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

        # If the cpu_total_time is 0, it means
        # that the function is likely too short to
        # get a good estimate of the CPU utilization
        # In that case, it might be safer to assume
        # that the CPU utilization is 1.0
        if cpu_total_time == 0:
            return 1.0

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
        # Prepare data potentially used
        # for indiviual log cost and carbon analysis
        analysis_data = {
            "input_payload_size_gb": self.input_payload_size,
            "download_size_gb": self.download_size,
            "consumed_read_capacity_from_download": self.consumed_read_capacity,
            "total_input_data_transfer_gb": self._get_total_input_data_size(),
            "total_output_data_transfer_gb": self._get_total_output_data_size(),
        }

        # Only return the fields that are not None
        result = {
            "instance_name": self.instance_name,
            "duration_s": self.longest_duration,
            "cpu_model": self.cpu_model,
            "provider_region": self.provider_region,
            "data_transfer_during_execution_gb": self.data_transfer_during_execution,
            "cpu_utilization": self.cpu_utilization,
            "successor_data": {k: v.to_dict() for k, v in self.successor_data.items()},
            "additional_analysis_data": {key: value for key, value in analysis_data.items() if value is not None},
        }

        # Filter out fields that are None
        filtered_result = {key: value for key, value in result.items() if value is not None and value != {}}

        return filtered_result
