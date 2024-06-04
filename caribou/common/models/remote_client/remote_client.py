import json
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Optional

from caribou.deployment.common.deploy.models.resource import Resource


class RemoteClient(ABC):  # pylint: disable=too-many-public-methods
    @abstractmethod
    def create_function(
        self,
        function_name: str,
        role_identifier: str,
        zip_contents: bytes,
        runtime: str,
        handler: str,
        environment_variables: dict[str, str],
        timeout: int,
        memory_size: int,
        additional_docker_commands: Optional[list[str]] = None,
    ) -> str:
        raise NotImplementedError()

    @abstractmethod
    def get_current_provider_region(self) -> str:
        raise NotImplementedError()

    @abstractmethod
    def resource_exists(self, resource: Resource) -> bool:
        raise NotImplementedError()

    @abstractmethod
    def create_role(self, role_name: str, policy: str, trust_policy: dict) -> str:
        raise NotImplementedError()

    @abstractmethod
    def update_role(self, role_name: str, policy: str, trust_policy: dict) -> str:
        raise NotImplementedError()

    def invoke_function(
        self,
        message: str,
        identifier: str,
        workflow_instance_id: str = "0",
        sync: bool = False,
        function_name: Optional[str] = None,
        expected_counter: int = -1,
        current_instance_name: Optional[str] = None,
        alternative_message: Optional[str] = None,
    ) -> tuple[Optional[float], Optional[float], bool, float, float]:
        # Returns the (size of uploaded sync data None if no data was uploaded,
        # if successor was invoked, RTT for dynamodb access, and the consumed write capacity),
        # In other words (payload_size, successor_invoked, RTT, consumed_write_capacity)
        uploaded_payload_size: Optional[float] = None
        sync_data_response_size: Optional[float] = None

        # If the successor was invoked
        successor_invoked = True

        # The RTT for a potential dynamodb upload (Only for sync nodes)
        upload_rtt = 0.0

        # Write consumed capacity is only used for sync nodes
        total_consumed_write_capacity = 0.0

        if sync:
            if function_name is None:
                raise RuntimeError("Function name must be specified for synchronization node")
            if expected_counter == -1:
                raise RuntimeError("Expected counter must be specified for synchronization node")
            if current_instance_name is None:
                raise RuntimeError("Current instance name must be specified for synchronization node")

            # Unpack message as we should only store the client data and not our added metadata, the added metadata is
            # still sent to the function using the messaging service upon calling
            # (so the workflow placement information is still forwarded)
            message_dictionary = json.loads(message)
            if "payload" not in message_dictionary:
                payload = ""
            payload = message_dictionary["payload"]
            json_payload = json.dumps(payload)
            uploaded_payload_size = len(json_payload.encode("utf-8")) / (1024**3)

            # Record the RTT time for uploading the data
            time_start = datetime.now()
            total_consumed_write_capacity += self.upload_predecessor_data_at_sync_node(
                function_name, workflow_instance_id, json_payload
            )
            time_end = datetime.now()

            # Calculate the time taken to upload the data
            upload_rtt = (time_end - time_start).total_seconds()

            # Update the predecessor reached states
            reached_states, sync_data_response_size, write_consumed_capacity = self.set_predecessor_reached(
                current_instance_name, function_name, workflow_instance_id, direct_call=True
            )
            total_consumed_write_capacity += write_consumed_capacity

            if len(reached_states) != expected_counter:
                successor_invoked = False
                return uploaded_payload_size, sync_data_response_size, successor_invoked, upload_rtt, total_consumed_write_capacity
        try:
            if sync and alternative_message is not None:
                message = alternative_message

            self.send_message_to_messaging_service(identifier, message)
        except Exception as e:
            raise RuntimeError(f"Could not invoke function through SNS: {str(e)}") from e

        return uploaded_payload_size, sync_data_response_size, successor_invoked, upload_rtt, total_consumed_write_capacity

    @abstractmethod
    def set_predecessor_reached(
        self, predecessor_name: str, sync_node_name: str, workflow_instance_id: str, direct_call: bool
    ) -> tuple[list[bool], float, float]:
        raise NotImplementedError()

    @abstractmethod
    def send_message_to_messaging_service(self, identifier: str, message: str) -> None:
        raise NotImplementedError()

    @abstractmethod
    def get_predecessor_data(
        self, current_instance_name: str, workflow_instance_id: str, consistent_read: bool = True
    ) -> tuple[list[str], float]:
        raise NotImplementedError()

    @abstractmethod
    def upload_predecessor_data_at_sync_node(
        self, function_name: str, workflow_instance_id: str, message: str
    ) -> float:
        raise NotImplementedError()

    @abstractmethod
    def set_value_in_table(self, table_name: str, key: str, value: str) -> None:
        raise NotImplementedError()

    @abstractmethod
    def update_value_in_table(self, table_name: str, key: str, value: str) -> None:
        raise NotImplementedError()

    @abstractmethod
    def set_value_in_table_column(
        self, table_name: str, key: str, column_type_value: list[tuple[str, str, str]]
    ) -> None:
        raise NotImplementedError()

    @abstractmethod
    def get_value_from_table(self, table_name: str, key: str, consistent_read: bool = True) -> tuple[str, float]:
        raise NotImplementedError()

    @abstractmethod
    def remove_value_from_table(self, table_name: str, key: str) -> None:
        raise NotImplementedError()

    @abstractmethod
    def get_all_values_from_table(self, table_name: str) -> dict[str, Any]:
        raise NotImplementedError()

    @abstractmethod
    def get_key_present_in_table(self, table_name: str, key: str, consistent_read: bool = True) -> bool:
        raise NotImplementedError()

    @abstractmethod
    def upload_resource(self, key: str, resource: bytes) -> None:
        raise NotImplementedError()

    @abstractmethod
    def download_resource(self, key: str) -> bytes:
        raise NotImplementedError()

    @abstractmethod
    def get_keys(self, table_name: str) -> list[str]:
        raise NotImplementedError()

    @abstractmethod
    def update_function(
        self,
        function_name: str,
        role_identifier: str,
        zip_contents: bytes,
        runtime: str,
        handler: str,
        environment_variables: dict[str, str],
        timeout: int,
        memory_size: int,
        additional_docker_commands: Optional[list[str]] = None,
    ) -> str:
        raise NotImplementedError()

    @abstractmethod
    def create_sync_tables(self) -> None:
        raise NotImplementedError()

    @abstractmethod
    def get_logs_since(self, function_instance: str, since: datetime) -> list[str]:
        raise NotImplementedError()

    @abstractmethod
    def get_logs_between(self, function_instance: str, start: datetime, end: datetime) -> list[str]:
        raise NotImplementedError()

    @abstractmethod
    def get_insights_logs_between(self, function_instance: str, start: datetime, end: datetime) -> list[str]:
        raise NotImplementedError()

    @abstractmethod
    def remove_key(self, table_name: str, key: str) -> None:
        raise NotImplementedError()

    @abstractmethod
    def remove_function(self, function_name: str) -> None:
        raise NotImplementedError()

    @abstractmethod
    def remove_role(self, role_name: str) -> None:
        raise NotImplementedError()

    @abstractmethod
    def remove_messaging_topic(self, topic_identifier: str) -> None:
        raise NotImplementedError()

    @abstractmethod
    def get_topic_identifier(self, topic_name: str) -> str:
        raise NotImplementedError()

    @abstractmethod
    def remove_resource(self, key: str) -> None:
        raise NotImplementedError()
