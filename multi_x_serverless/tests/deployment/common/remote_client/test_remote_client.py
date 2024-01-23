import unittest
from unittest.mock import Mock
import json

from multi_x_serverless.deployment.common.remote_client.remote_client import RemoteClient


class TestRemoteClient(unittest.TestCase):
    class MockRemoteClient(RemoteClient):
        def create_function(self):
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

        def increment_counter(self, function_name, workflow_instance_id):
            return 1

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

    def test_invoke_function(self):
        client = self.MockRemoteClient()
        client.send_message_to_messaging_service = Mock()
        client.increment_counter = Mock(return_value=1)
        client.upload_predecessor_data_at_sync_node = Mock()

        message = json.dumps({"payload": "test"})
        client.invoke_function(message, "identifier", "workflow_instance_id", True, "function_name", 1)

        client.send_message_to_messaging_service.assert_called_once_with("identifier", message)
        client.increment_counter.assert_called_once_with("function_name", "workflow_instance_id")
        client.upload_predecessor_data_at_sync_node.assert_called_once_with(
            "function_name", "workflow_instance_id", json.dumps("test")
        )
