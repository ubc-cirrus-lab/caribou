from abc import ABC, abstractmethod
from typing import Any

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
    def resource_exists(self, resource: Resource) -> bool:
        raise NotImplementedError()

    @abstractmethod
    def create_role(self, role_name: str, policy: str, trust_policy: dict) -> str:
        raise NotImplementedError()

    @abstractmethod
    def update_role(self, role_name: str, policy: str, trust_policy: dict) -> str:
        raise NotImplementedError()

    @abstractmethod
    def invoke_function(self, message: str, region: str, identifier: str, merge: bool = False) -> None:
        raise NotImplementedError()

    @abstractmethod
    def get_predecessor_data(self, current_instance_name: str, workflow_instance_id: str) -> list[dict[str, Any]]:
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
