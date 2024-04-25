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
    ) -> None:
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
            self.upload_predecessor_data_at_sync_node(function_name, workflow_instance_id, json_payload)
            reached_states = self.set_predecessor_reached(
                current_instance_name, function_name, workflow_instance_id, direct_call=True
            )

            if len(reached_states) != expected_counter:
                return
        try:
            self.send_message_to_messaging_service(identifier, message)
        except Exception as e:
            raise RuntimeError(f"Could not invoke function through SNS: {str(e)}") from e

    @abstractmethod
    def set_predecessor_reached(
        self, predecessor_name: str, sync_node_name: str, workflow_instance_id: str, direct_call: bool
    ) -> list[bool]:
        raise NotImplementedError()

    @abstractmethod
    def send_message_to_messaging_service(self, identifier: str, message: str) -> None:
        raise NotImplementedError()

    @abstractmethod
    def get_predecessor_data(self, current_instance_name: str, workflow_instance_id: str) -> list[str]:
        raise NotImplementedError()

    @abstractmethod
    def upload_predecessor_data_at_sync_node(self, function_name: str, workflow_instance_id: str, message: str) -> None:
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
    def get_value_from_table(self, table_name: str, key: str) -> str:
        raise NotImplementedError()

    @abstractmethod
    def remove_value_from_table(self, table_name: str, key: str) -> None:
        raise NotImplementedError()

    @abstractmethod
    def get_all_values_from_table(self, table_name: str) -> dict[str, Any]:
        raise NotImplementedError()

    @abstractmethod
    def get_key_present_in_table(self, table_name: str, key: str) -> bool:
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