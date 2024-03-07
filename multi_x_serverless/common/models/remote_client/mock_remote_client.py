from datetime import datetime
from typing import Optional

from multi_x_serverless.common.models.remote_client.remote_client import RemoteClient


class MockRemoteClient(RemoteClient):  # pylint: disable=too-many-public-methods
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
        pass

    def resource_exists(self, resource):
        pass

    def get_current_provider_region(self) -> str:
        pass

    def create_role(self, role_name, policy, trust_policy):
        pass

    def update_role(self, role_name, policy, trust_policy):
        pass

    def send_message_to_messaging_service(self, identifier, message):
        pass

    def get_predecessor_data(self, current_instance_name, workflow_instance_id):
        pass

    def upload_predecessor_data_at_sync_node(self, function_name, workflow_instance_id, message):
        pass

    def set_value_in_table(self, table_name, key, value):
        pass

    def get_value_from_table(self, table_name, key):
        pass

    def upload_resource(self, key, resource):
        pass

    def download_resource(self, key):
        pass

    def update_function(
        self,
        function_name,
        role_identifier,
        zip_contents,
        runtime,
        handler,
        environment_variables,
        timeout,
        memory_size,
        additional_docker_commands: Optional[list[str]] = None,
    ):
        pass

    def get_key_present_in_table(self, table_name: str, key: str) -> bool:
        pass

    def set_predecessor_reached(
        self, predecessor_name: str, sync_node_name: str, workflow_instance_id: str, direct_call: bool
    ) -> list[bool]:
        pass

    def get_all_values_from_table(self, table_name: str) -> dict:
        pass

    def set_value_in_table_column(
        self, table_name: str, key: str, column_type_value: list[tuple[str, str, str]]
    ) -> None:
        pass

    def get_keys(self, table_name: str) -> list[str]:
        pass

    def remove_value_from_table(self, table_name: str, key: str) -> None:
        pass

    def create_sync_tables(self) -> None:
        pass

    def get_logs_since(self, function_instance: str, since: datetime) -> list[str]:
        pass

    def remove_key(self, table_name: str, key: str) -> None:
        pass

    def remove_function(self, function_name: str) -> None:
        pass

    def remove_role(self, role_name: str) -> None:
        pass

    def remove_messaging_topic(self, topic_identifier: str) -> None:
        pass

    def get_topic_identifier(self, topic_name: str) -> str:
        pass

    def remove_resource(self, key: str) -> None:
        pass

    def update_value_in_table(self, table_name: str, key: str, value: str) -> None:
        pass
