from typing import Any, Optional
from caribou.syncers.components.execution_data import ExecutionData

class StartHopData:  # pylint: disable=too-many-instance-attributes
    def __init__(self) -> None:
        # Required fields
        # self.first_execution_instance_name: Optional[str] = None
        self.request_source: Optional[str] = None
        self.destination_provider_region: Optional[str] = None
        self.data_transfer_size: Optional[float] = None
        self.wpd_data_size: Optional[float] = None
        self.consumed_read_capacity: Optional[float] = None
        self.init_latency_from_first_recieved: Optional[float] = None
        self.start_hop_latency_from_client: Optional[float] = None

        
        # # Start hop informations
        # self.start_hop_latency: float = 0.0
        # self.start_hop_data_transfer_size: float = 0.0
        # self.start_hop_instance_name: Optional[str] = None
        # self.start_hop_destination: Optional[str] = None
        # self.start_hop_wpd_data_size: Optional[float] = None
        # self.start_hop_wpd_consumed_read_capacity: Optional[float] = None


        # Optional Fields, only if redirected
        ## The name of the instance (Code name, as the real instance name
        ## is simply the same as the name of the first execution instance)
        ## If a request was not redirected, this is set to None
        self.redirector_execution_data: Optional[ExecutionData] = None

        ## Used for ensuring that we only observe workflow samples
        ## that does not have duplicate invocations in the same run.
        self.encountered_request_ids: set[str] = set()

    def get_redirector_execution_data(self, instance_name: str, request_id: Optional[str]) -> ExecutionData:
        # Add the request ID to the encountered request IDs
        # This is to blacklist runs that have duplicate invocations
        # As they distort the data.
        if request_id is not None:
            self.encountered_request_ids.add(request_id)

        if not self.redirector_execution_data:
            self.redirector_execution_data = ExecutionData(instance_name)
        elif self.redirector_execution_data.instance_name != instance_name:
            raise ValueError(
                f"Redirector instance name '{instance_name}' does not match existing instance name '{self.redirector_execution_data.instance_name}'"
            )

        return self.redirector_execution_data

    @property
    def is_completed(self) -> bool:
        # For required fields
        required_fields_completed: bool = all(
            [
                self.request_source is not None,
                self.destination_provider_region is not None,
                self.data_transfer_size is not None,
                self.wpd_data_size is not None,
                self.consumed_read_capacity is not None,
                self.init_latency_from_first_recieved is not None,
                self.start_hop_latency_from_client is not None,
            ]
        )

        # For redirected fields (Redirector Only)
        redirected_fields_completed: bool = True
        if self.redirector_execution_data is not None:
            redirected_fields_completed = self.redirector_execution_data.is_completed if len(self.encountered_request_ids) == 1 else False

        return required_fields_completed and redirected_fields_completed

    def to_dict(self) -> dict[str, Any]:
        # # Prepare data potentially used
        # # for indiviual log cost and carbon analysis
        # redirector_data = {
        #     "input_payload_size_gb": self.input_payload_size,
        #     "download_size_gb": self.download_size,
        #     "consumed_read_capacity_from_download": self.consumed_read_capacity,
        #     "total_input_data_transfer_gb": self._get_total_input_data_size(),
        #     "total_output_data_transfer_gb": self._get_total_output_data_size(),
        # }

        # # Only return the fields that are not None
        # result = {
        #     "instance_name": self.instance_name,
        #     "duration_s": self.longest_duration,
        #     "cpu_model": self.cpu_model,
        #     "provider_region": self.provider_region,
        #     "data_transfer_during_execution_gb": self.data_transfer_during_execution,
        #     "cpu_utilization": self.cpu_utilization,
        #     "successor_data": {k: v.to_dict() for k, v in self.successor_data.items()},
        #     "additional_analysis_data": {key: value for key, value in analysis_data.items() if value is not None},
        # }

        # # Filter out fields that are None
        # filtered_result = {key: value for key, value in result.items() if value is not None and value != {}}

        # return filtered_result
        return {} # TODO: Implement