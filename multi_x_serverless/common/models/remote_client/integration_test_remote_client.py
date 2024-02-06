from typing import Any

from multi_x_serverless.common.models.remote_client.remote_client import RemoteClient
from multi_x_serverless.deployment.common.deploy.models.resource import Resource


class IntegrationTestRemoteClient(RemoteClient):
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
        return ""

    def resource_exists(self, resource: Resource) -> bool:
        return False

    def create_role(self, role_name: str, policy: str, trust_policy: dict) -> str:
        return ""

    def update_role(self, role_name: str, policy: str, trust_policy: dict) -> str:
        return ""

    def send_message_to_messaging_service(self, identifier: str, message: str) -> None:
        pass

    def get_predecessor_data(self, current_instance_name: str, workflow_instance_id: str) -> list[str]:
        return []

    def upload_predecessor_data_at_sync_node(self, function_name: str, workflow_instance_id: str, message: str) -> None:
        pass

    def set_value_in_table(self, table_name: str, key: str, value: str) -> None:
        pass

    def get_value_from_table(self, table_name: str, key: str) -> str:
        return ""

    def upload_resource(self, key: str, resource: bytes) -> None:
        pass

    def download_resource(self, key: str) -> bytes:
        return b""

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
        return ""

    def get_key_present_in_table(self, table_name: str, key: str) -> bool:
        return False

    def set_predecessor_reached(self, predecessor_name: str, sync_node_name: str, workflow_instance_id: str) -> int:
        return 1

    def get_all_values_from_table(self, table_name: str) -> dict:
        return {}

    def set_value_in_table_column(
        self, table_name: str, key: str, column_type_value: list[tuple[str, str, str]]
    ) -> None:
        pass

    def get_all_values_from_sort_key_table(self, table_name: str, key: str) -> list[dict[str, Any]]:
        return []

    def get_keys(self, table_name: str) -> list[str]:
        return []

    def remove_value_from_table(self, table_name: str, key: str) -> None:
        pass
