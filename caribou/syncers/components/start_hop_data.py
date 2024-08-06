from typing import Any, Optional

from caribou.syncers.components.execution_data import ExecutionData


class StartHopData:  # pylint: disable=too-many-instance-attributes
    def __init__(self) -> None:
        # Required fields
        # self.first_execution_instance_name: Optional[str] = None
        self.request_source: Optional[str] = None
        self.destination_provider_region: Optional[str] = None

        # Indicate how much data is transfered to the first function (in GB)
        # The redirector is NOT included in this calculation (Aka we don't
        # consider it as a function)
        self.input_payload_size_to_first_function: Optional[float] = None
        self.wpd_data_size: Optional[float] = None
        self.consumed_read_capacity: Optional[float] = None

        # Debug only message, this can include the time from the first
        # request to the first function, or the time or the time from
        # the redirector to the first function.
        self.init_latency_from_first_recieved: Optional[float] = None

        # Indicate when it arrive at the ENTRY point of the function
        # from when the function was started (Not from the client or
        # redirector) This mostly capture the time from pulling wpd
        # and or checking if it needs to be pulled.
        self.time_from_function_start_to_entry_point: Optional[float] = None

        # This indicate when the request was received by the first function
        # Which CAN be a redirector. This is used to calculate the start hop
        # from client latency.
        self.start_hop_latency_from_client: Optional[float] = None

        # Optional Fields, used only in the case the user wishes
        # to override the workflow placement decision.
        self.overridden_wpd_data_size: Optional[float] = None

        # Indicate if the placement decision was retrieved from the platform
        # Or if it were cached
        self.retrieved_wpd_at_function: bool = False

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

        # Set the request ID if it is not set
        if self.redirector_execution_data.request_id is None:
            self.redirector_execution_data.request_id = request_id

        elif self.redirector_execution_data.instance_name != instance_name:
            raise ValueError(
                f"Redirector instance name '{instance_name}' does not match "
                f"existing instance name '{self.redirector_execution_data.instance_name}'"
            )

        return self.redirector_execution_data

    @property
    def is_completed(self) -> bool:
        # For required fields
        required_fields_completed: bool = all(
            [
                self.request_source is not None,
                self.destination_provider_region is not None,
                self.input_payload_size_to_first_function is not None,
                self.wpd_data_size is not None,
                self.consumed_read_capacity is not None,
                self.time_from_function_start_to_entry_point is not None,
                self.start_hop_latency_from_client is not None,
            ]
        )

        # For redirected fields (Redirector Only)
        redirected_fields_completed: bool = True
        if self.redirector_execution_data is not None:
            redirected_fields_completed = (
                self.redirector_execution_data.is_completed if len(self.encountered_request_ids) == 1 else False
            )

        return required_fields_completed and redirected_fields_completed

    def to_dict(self) -> dict[str, Any]:
        workflow_placement_decision = {
            "data_size_gb": self.wpd_data_size,
            "overridden_data_size_gb": self.overridden_wpd_data_size,
            "consumed_read_capacity": self.consumed_read_capacity,
            "retrieved_wpd_at_function": self.retrieved_wpd_at_function,
        }

        # Filter out fields that are None
        filtered_workflow_placement_decision = {
            key: value for key, value in workflow_placement_decision.items() if value is not None
        }

        start_hop_info = {
            "destination": self.destination_provider_region,
            "request_source": self.request_source,
            "data_transfer_size_gb": self.input_payload_size_to_first_function,
            "latency_from_client_s": self.start_hop_latency_from_client,
            "time_from_function_start_to_entry_point_s": self.time_from_function_start_to_entry_point,
            "workflow_placement_decision": filtered_workflow_placement_decision,
            "redirector_execution_data": self.redirector_execution_data.to_dict()
            if self.redirector_execution_data
            else None,
        }

        # Filter out fields that are None
        filtered_result = {key: value for key, value in start_hop_info.items() if value is not None and value != {}}

        return filtered_result
