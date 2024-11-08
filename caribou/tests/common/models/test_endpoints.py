import unittest
from unittest.mock import patch, MagicMock
from caribou.common.models.remote_client.aws_remote_client import AWSRemoteClient
from caribou.common.models.remote_client.remote_client import RemoteClient
from caribou.common.models.remote_client.remote_client_factory import RemoteClientFactory
from caribou.common.provider import Provider
from caribou.common.constants import GLOBAL_SYSTEM_REGION, INTEGRATION_TEST_SYSTEM_REGION
from caribou.common.models.endpoints import Endpoints  # Adjust the import as needed


class TestEndpoints(unittest.TestCase):
    @patch.object(RemoteClientFactory, "get_remote_client")
    @patch.object(RemoteClientFactory, "get_framework_cli_remote_client")
    def test_initialization(self, mock_get_framework_cli_remote_client, mock_get_remote_client):
        # Setup environment for the test
        mock_get_remote_client.return_value = MagicMock(spec=RemoteClient)
        mock_get_framework_cli_remote_client.return_value = MagicMock(spec=AWSRemoteClient)

        # Case 1: INTEGRATIONTEST_ON is False
        with patch.dict("os.environ", {"INTEGRATIONTEST_ON": "False"}):
            endpoints = Endpoints()

            # Assertions for AWS provider
            mock_get_remote_client.assert_any_call(Provider.AWS.value, GLOBAL_SYSTEM_REGION)
            self.assertEqual(endpoints.get_deployment_resources_client(), mock_get_remote_client.return_value)
            self.assertEqual(endpoints.get_deployment_manager_client(), mock_get_remote_client.return_value)
            self.assertEqual(
                endpoints.get_deployment_algorithm_workflow_placement_decision_client(),
                mock_get_remote_client.return_value,
            )
            self.assertEqual(endpoints.get_data_collector_client(), mock_get_remote_client.return_value)
            self.assertEqual(endpoints.get_datastore_client(), mock_get_remote_client.return_value)

        # Case 2: INTEGRATIONTEST_ON is True
        with patch.dict("os.environ", {"INTEGRATIONTEST_ON": "True"}):
            endpoints = Endpoints()

            # Assertions for Integration Test provider
            mock_get_remote_client.assert_any_call(
                Provider.INTEGRATION_TEST_PROVIDER.value, INTEGRATION_TEST_SYSTEM_REGION
            )
            self.assertEqual(endpoints.get_deployment_resources_client(), mock_get_remote_client.return_value)
            self.assertEqual(endpoints.get_deployment_manager_client(), mock_get_remote_client.return_value)
            self.assertEqual(
                endpoints.get_deployment_algorithm_workflow_placement_decision_client(),
                mock_get_remote_client.return_value,
            )
            self.assertEqual(endpoints.get_data_collector_client(), mock_get_remote_client.return_value)
            self.assertEqual(endpoints.get_datastore_client(), mock_get_remote_client.return_value)

    def test_get_deployment_resources_client(self):
        with patch.object(
            RemoteClientFactory, "get_remote_client", return_value=MagicMock(spec=RemoteClient)
        ) as mock_get_remote_client:
            endpoints = Endpoints()
            self.assertEqual(endpoints.get_deployment_resources_client(), mock_get_remote_client.return_value)

    def test_get_deployment_manager_client(self):
        with patch.object(
            RemoteClientFactory, "get_remote_client", return_value=MagicMock(spec=RemoteClient)
        ) as mock_get_remote_client:
            endpoints = Endpoints()
            self.assertEqual(endpoints.get_deployment_manager_client(), mock_get_remote_client.return_value)

    def test_get_deployment_algorithm_workflow_placement_decision_client(self):
        with patch.object(
            RemoteClientFactory, "get_remote_client", return_value=MagicMock(spec=RemoteClient)
        ) as mock_get_remote_client:
            endpoints = Endpoints()
            self.assertEqual(
                endpoints.get_deployment_algorithm_workflow_placement_decision_client(),
                mock_get_remote_client.return_value,
            )

    def test_get_data_collector_client(self):
        with patch.object(
            RemoteClientFactory, "get_remote_client", return_value=MagicMock(spec=RemoteClient)
        ) as mock_get_remote_client:
            endpoints = Endpoints()
            self.assertEqual(endpoints.get_data_collector_client(), mock_get_remote_client.return_value)

    def test_get_datastore_client(self):
        with patch.object(
            RemoteClientFactory, "get_remote_client", return_value=MagicMock(spec=RemoteClient)
        ) as mock_get_remote_client:
            endpoints = Endpoints()
            self.assertEqual(endpoints.get_datastore_client(), mock_get_remote_client.return_value)

    def test_get_framework_cli_remote_client(self):
        with patch.object(
            RemoteClientFactory, "get_framework_cli_remote_client", return_value=MagicMock(spec=AWSRemoteClient)
        ) as mock_get_framework_cli_remote_client:
            endpoints = Endpoints()
            self.assertEqual(
                endpoints.get_framework_cli_remote_client(), mock_get_framework_cli_remote_client.return_value
            )


if __name__ == "__main__":
    unittest.main()
