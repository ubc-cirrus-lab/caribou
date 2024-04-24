import unittest
from unittest.mock import Mock
import json

from caribou.common.models.remote_client.mock_remote_client import MockRemoteClient


class TestRemoteClient(unittest.TestCase):
    def test_invoke_function(self):
        client = MockRemoteClient()
        client.send_message_to_messaging_service = Mock()
        client.set_predecessor_reached = Mock(return_value=[True])
        client.upload_predecessor_data_at_sync_node = Mock()

        message = json.dumps({"payload": "test"})
        client.invoke_function(
            message, "identifier", "workflow_instance_id", True, "sync_node_name", 1, "function_name"
        )

        client.send_message_to_messaging_service.assert_called_once_with("identifier", message)
        client.set_predecessor_reached.assert_called_once_with(
            "function_name", "sync_node_name", "workflow_instance_id", direct_call=True
        )
        client.upload_predecessor_data_at_sync_node.assert_called_once_with(
            "sync_node_name", "workflow_instance_id", '"test"'
        )


if __name__ == "__main__":
    unittest.main()
