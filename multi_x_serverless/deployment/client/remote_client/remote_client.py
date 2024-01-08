from abc import ABC, abstractmethod
from typing import Any, Optional

from multi_x_serverless.deployment.client.deploy.models.resource import Resource


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
    def get_iam_role(self, role_name: str) -> str:
        raise NotImplementedError()

    @abstractmethod
    def resource_exists(self, resource: Resource) -> bool:
        raise NotImplementedError()

    @abstractmethod
    def get_lambda_function(self, function_name: str) -> dict[str, Any]:
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
        merge: bool = False,
        function_name: Optional[str] = None,
        expected_counter: int = -1,
    ) -> None:
        if merge:
            if function_name is None:
                raise RuntimeError("Function name must be specified for merge")
            if expected_counter == -1:
                raise RuntimeError("Expected counter must be specified for merge")

            self.upload_message_for_merge(function_name, workflow_instance_id, message)
            counter = self.increment_counter(function_name, workflow_instance_id)

            if counter != expected_counter:
                return
        try:
            self.send_message_to_messaging_service(identifier, message)
        except Exception as e:
            raise RuntimeError("Could not invoke function through SNS") from e

    @abstractmethod
    def send_message_to_messaging_service(self, identifier: str, message: str) -> None:
        raise NotImplementedError()

    @abstractmethod
    def get_predecessor_data(self, current_instance_name: str, workflow_instance_id: str) -> list[dict[str, Any]]:
        raise NotImplementedError()

    @abstractmethod
    def increment_counter(self, function_name: str, workflow_instance_id: str) -> int:
        raise NotImplementedError()

    @abstractmethod
    def upload_message_for_merge(self, function_name: str, workflow_instance_id: str, message: str) -> None:
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
