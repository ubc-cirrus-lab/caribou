from typing import Any

from multi_x_serverless.common.models.remote_client.remote_client import RemoteClient


class MockRemoteClient(RemoteClient):
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
        pass

    def resource_exists(self, resource):
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
        self, function_name, role_arn, zip_contents, runtime, handler, environment_variables, timeout, memory_size
    ):
        pass

    def get_key_present_in_table(self, table_name: str, key: str) -> bool:
        pass

    def set_predecessor_reached(self, predecessor_name: str, sync_node_name: str, workflow_instance_id: str) -> int:
        pass

    def get_all_values_from_table(self, table_name: str) -> dict:
        pass

    def set_value_in_table_column(
        self, table_name: str, key: str, column_type_value: list[tuple[str, str, str]]
    ) -> None:
        pass

    def get_all_values_from_sort_key_table(self, table_name: str, key: str) -> list[dict[str, Any]]:
        pass
