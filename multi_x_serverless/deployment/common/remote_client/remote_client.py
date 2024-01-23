import json
from abc import ABC, abstractmethod
from typing import Optional

from multi_x_serverless.deployment.common.deploy.models.resource import Resource


class RemoteClient(ABC):
    @abstractmethod
    def create_function(
        self,
        function_name: str,
        role_arn: str,
        zip_contents: bytes,
        runtime: str,
        handler: str,
        environment_variables: dict[str, str],
        timeout: int,
        memory_size: int,
    ) -> str:
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
        workflow_instance_id: str,
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
            counter = self.set_predecessor_reached(current_instance_name, function_name, workflow_instance_id)

            if counter != expected_counter:
                return
        try:
            self.send_message_to_messaging_service(identifier, message)
        except Exception as e:
            raise RuntimeError("Could not invoke function through SNS") from e

    @abstractmethod
    def set_predecessor_reached(self, predecessor_name: str, sync_node_name: str, workflow_instance_id: str) -> int:
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
    def get_value_from_table(self, table_name: str, key: str) -> str:
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
    def update_function(
        self,
        function_name: str,
        role_arn: str,
        zip_contents: bytes,
        runtime: str,
        handler: str,
        environment_variables: dict[str, str],
        timeout: int,
        memory_size: int,
    ) -> str:
        raise NotImplementedError()
