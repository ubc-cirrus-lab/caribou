import unittest
from unittest.mock import patch, MagicMock
from multi_x_serverless.syncers.datastore_syncer import DatastoreSyncer

import unittest
from unittest.mock import MagicMock, patch


class TestDatastoreSyncer(unittest.TestCase):
    def setUp(self):
        self.syncer = DatastoreSyncer()

    def test_initialize_workflow_summary_instance(self):
        result = self.syncer.initialize_workflow_summary_instance()
        self.assertEqual(result, {"instance_summary": {}})

    def test_get_last_synced_time(self):
        with patch.object(
            self.syncer.endpoints.get_datastore_client(), "get_last_value_from_sort_key_table"
        ) as mock_get_last_value:
            mock_get_last_value.return_value = ["2022-01-01 00:00:00.000000"]
            result = self.syncer.get_last_synced_time("workflow_id")
            self.assertEqual(result.year, 2022)

    def test_validate_deployment_manager_config(self):
        with self.assertRaises(Exception):
            self.syncer.validate_deployment_manager_config({}, "workflow_id")

    def test_initialize_instance_summary(self):
        workflow_summary_instance = {"instance_summary": {}}
        function_instance = "function1"
        provider_region = {"provider": "aws", "region": "us-east-1"}
        self.syncer.initialize_instance_summary(function_instance, provider_region, workflow_summary_instance)
        self.assertIn(function_instance, workflow_summary_instance["instance_summary"])

    def test_process_logs(self):
        logs = [
            "ENTRY_POINT",
            "CALLED",
            "Billed Duration: 100",
            "CALLED",
            "Billed Duration: 200",
        ]
        function_instance = "function1"
        provider_region = {"provider": "aws", "region": "us-east-1"}
        workflow_summary_instance = {
            "instance_summary": {
                function_instance: {
                    "invocation_count": 0,
                    "execution_summary": {
                        f'{provider_region["provider"]}:{provider_region["region"]}': {
                            "invocation_count": 0,
                            "average_runtime": 0,
                            "tail_runtime": 0,
                        }
                    },
                }
            }
        }
        entry_point_invocation_count = self.syncer.process_logs(
            logs, function_instance, provider_region, workflow_summary_instance
        )
        self.assertEqual(entry_point_invocation_count, 1)
        self.assertEqual(workflow_summary_instance["instance_summary"][function_instance]["invocation_count"], 2)
        self.assertEqual(
            workflow_summary_instance["instance_summary"][function_instance]["execution_summary"][
                f'{provider_region["provider"]}:{provider_region["region"]}'
            ]["invocation_count"],
            2,
        )
        self.assertEqual(
            workflow_summary_instance["instance_summary"][function_instance]["execution_summary"][
                f'{provider_region["provider"]}:{provider_region["region"]}'
            ]["average_runtime"],
            150,
        )
        self.assertEqual(
            workflow_summary_instance["instance_summary"][function_instance]["execution_summary"][
                f'{provider_region["provider"]}:{provider_region["region"]}'
            ]["tail_runtime"],
            200,
        )

    @patch("multi_x_serverless.common.models.endpoints.Endpoints")
    @patch("multi_x_serverless.common.models.remote_client.remote_client_factory.RemoteClientFactory")
    def test_sync(self, mock_remote_client_factory, mock_endpoints):
        # Create mock objects for the methods that will be called
        mock_get_deployment_manager_client = MagicMock()
        mock_get_all_values_from_table = MagicMock()
        mock_get_deployment_manager_client.get_all_values_from_table = mock_get_all_values_from_table
        mock_endpoints.get_deployment_manager_client = mock_get_deployment_manager_client

        mock_get_remote_client = MagicMock()
        mock_remote_client_factory.get_remote_client = mock_get_remote_client

        mock_remote_client = MagicMock()
        mock_get_remote_client.return_value = mock_remote_client

        # Create a DatastoreSyncer instance and call the sync method
        syncer = DatastoreSyncer()
        syncer.endpoints = mock_endpoints
        syncer.sync()

        # Check if the methods were called with the correct arguments
        mock_get_deployment_manager_client.assert_called_once()
        mock_get_all_values_from_table.assert_called_once_with("DEPLOYMENT_MANAGER_RESOURCE_TABLE")
        mock_get_remote_client.assert_called()


if __name__ == "__main__":
    unittest.main()
