import unittest
from unittest.mock import patch, MagicMock
from multi_x_serverless.syncers.datastore_syncer import DatastoreSyncer
from multi_x_serverless.common.constants import DEPLOYMENT_MANAGER_RESOURCE_TABLE

import unittest
from unittest.mock import MagicMock, patch
import json
from datetime import datetime


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
            "INVOKED INSTANCE (function1)",
            "EXECUTED INSTANCE (function1) TIME (100)",
            "INVOKED INSTANCE (function1)",
            "EXECUTED INSTANCE (function1) TIME (200)",
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
                    "invocation_summary": {},
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
            0.15,
        )
        self.assertEqual(
            workflow_summary_instance["instance_summary"][function_instance]["execution_summary"][
                f'{provider_region["provider"]}:{provider_region["region"]}'
            ]["tail_runtime"],
            0.2,
        )

    def test_process_logs_with_invocation_summary(self):
        logs = [
            "ENTRY_POINT",
            "INVOKED INSTANCE (function1)",
            "EXECUTED INSTANCE (function1) TIME (100)",
            "INVOKED INSTANCE (function1)",
            "EXECUTED INSTANCE (function1) TIME (200)",
            "INVOKING_SUCCESSOR: INSTANCE (function1) calling SUCCESSOR (function2) with PAYLOAD_SIZE (1) GB",
            "INVOKING_SUCCESSOR: INSTANCE (function1) calling SUCCESSOR (function2) with PAYLOAD_SIZE (2) GB",
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
                    "invocation_summary": {},
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
            0.15,
        )
        self.assertEqual(
            workflow_summary_instance["instance_summary"][function_instance]["execution_summary"][
                f'{provider_region["provider"]}:{provider_region["region"]}'
            ]["tail_runtime"],
            0.2,
        )
        self.assertEqual(
            workflow_summary_instance["instance_summary"][function_instance]["invocation_summary"]["function2"][
                "invocation_count"
            ],
            2,
        )
        self.assertEqual(
            workflow_summary_instance["instance_summary"][function_instance]["invocation_summary"]["function2"][
                "average_data_transfer_size"
            ],
            1.5,
        )

    def test_process_logs_with_invocation_summary(self):
        logs = [
            "ENTRY_POINT",
            "INVOKED INSTANCE (function1)",
            "EXECUTED INSTANCE (function1) TIME (100)",
            "INVOKED INSTANCE (function1)",
            "EXECUTED INSTANCE (function1) TIME (200)",
            "INVOKING_SUCCESSOR: INSTANCE (function1) calling SUCCESSOR (function2) with PAYLOAD_SIZE (1) GB",
            "INVOKING_SUCCESSOR: INSTANCE (function1) calling SUCCESSOR (function2) with PAYLOAD_SIZE (2) GB",
            "INVOKED INSTANCE (function2)",
            "EXECUTED INSTANCE (function2) TIME (300)",
            "INVOKED INSTANCE (function2)",
            "EXECUTED INSTANCE (function2) TIME (150)",
            "INVOKING_SUCCESSOR: INSTANCE (function2) calling SUCCESSOR (function3) with PAYLOAD_SIZE (4) GB",
        ]
        provider_region = {"provider": "aws", "region": "us-east-1"}
        workflow_summary_instance = {"instance_summary": {}}

        entry_point_invocation_count = self.syncer.process_logs(
            logs, "function1", provider_region, workflow_summary_instance
        )
        self.assertEqual(entry_point_invocation_count, 1)
        self.assertEqual(workflow_summary_instance["instance_summary"]["function1"]["invocation_count"], 2)
        self.assertEqual(
            workflow_summary_instance["instance_summary"]["function1"]["execution_summary"][
                f'{provider_region["provider"]}:{provider_region["region"]}'
            ]["invocation_count"],
            2,
        )
        self.assertEqual(
            workflow_summary_instance["instance_summary"]["function1"]["execution_summary"][
                f'{provider_region["provider"]}:{provider_region["region"]}'
            ]["average_runtime"],
            0.15,
        )
        self.assertEqual(
            workflow_summary_instance["instance_summary"]["function1"]["execution_summary"][
                f'{provider_region["provider"]}:{provider_region["region"]}'
            ]["tail_runtime"],
            0.2,
        )
        self.assertEqual(
            workflow_summary_instance["instance_summary"]["function1"]["invocation_summary"]["function2"][
                "invocation_count"
            ],
            2,
        )
        self.assertEqual(
            workflow_summary_instance["instance_summary"]["function1"]["invocation_summary"]["function2"][
                "average_data_transfer_size"
            ],
            1.5,
        )
        self.assertEqual(workflow_summary_instance["instance_summary"]["function2"]["invocation_count"], 2)
        self.assertEqual(
            workflow_summary_instance["instance_summary"]["function2"]["execution_summary"][
                f'{provider_region["provider"]}:{provider_region["region"]}'
            ]["invocation_count"],
            2,
        )
        self.assertEqual(
            workflow_summary_instance["instance_summary"]["function2"]["execution_summary"][
                f'{provider_region["provider"]}:{provider_region["region"]}'
            ]["average_runtime"],
            0.225,
        )
        self.assertEqual(
            workflow_summary_instance["instance_summary"]["function2"]["execution_summary"][
                f'{provider_region["provider"]}:{provider_region["region"]}'
            ]["tail_runtime"],
            0.3,
        )
        self.assertEqual(
            workflow_summary_instance["instance_summary"]["function2"]["invocation_summary"]["function3"][
                "invocation_count"
            ],
            1,
        )
        self.assertEqual(
            workflow_summary_instance["instance_summary"]["function2"]["invocation_summary"]["function3"][
                "average_data_transfer_size"
            ],
            4,
        )

    @patch("multi_x_serverless.common.models.endpoints.Endpoints")
    @patch("multi_x_serverless.common.models.remote_client.remote_client_factory.RemoteClientFactory")
    def test_sync(self, mock_remote_client_factory, mock_endpoints):
        # Create mock objects for the methods that will be called
        mock_get_deployment_manager_client = MagicMock()
        mock_get_deployment_manager_client.get_all_values_from_table.return_value = {}
        mock_endpoints.get_deployment_manager_client.return_value = mock_get_deployment_manager_client

        mock_get_remote_client = MagicMock()
        mock_remote_client_factory.get_remote_client = mock_get_remote_client

        mock_remote_client = MagicMock()
        mock_get_remote_client.return_value = mock_remote_client

        # Create a DatastoreSyncer instance and call the sync method
        syncer = DatastoreSyncer()
        syncer.endpoints = mock_endpoints
        syncer.sync()

        # Check if the methods were called with the correct arguments
        mock_endpoints.get_deployment_manager_client.assert_called_once()
        mock_get_deployment_manager_client.get_all_values_from_table.assert_called_once_with(
            DEPLOYMENT_MANAGER_RESOURCE_TABLE
        )

    @patch.object(DatastoreSyncer, "process_workflow")
    @patch.object(DatastoreSyncer, "get_last_synced_time")
    @patch.object(DatastoreSyncer, "initialize_workflow_summary_instance")
    @patch("multi_x_serverless.common.models.endpoints.Endpoints")
    @patch("multi_x_serverless.common.models.remote_client.remote_client_factory.RemoteClientFactory")
    def test_sync(
        self,
        mock_remote_client_factory,
        mock_endpoints,
        mock_initialize_workflow_summary_instance,
        mock_get_last_synced_time,
        mock_process_workflow,
    ):
        # Mocking the scenario where the sync method is called successfully
        mock_get_deployment_manager_client = MagicMock()
        mock_get_deployment_manager_client.get_all_values_from_table.return_value = {
            "workflow_id": "deployment_manager_config_json"
        }
        mock_endpoints.get_deployment_manager_client.return_value = mock_get_deployment_manager_client

        mock_get_remote_client = MagicMock()
        mock_remote_client_factory.get_remote_client = mock_get_remote_client

        mock_remote_client = MagicMock()
        mock_get_remote_client.return_value = mock_remote_client

        mock_initialize_workflow_summary_instance.return_value = {"instance_summary": {}}
        mock_get_last_synced_time.return_value = datetime(2022, 1, 1)

        # Create a DatastoreSyncer instance and call the sync method
        syncer = DatastoreSyncer()
        syncer.endpoints = mock_endpoints
        syncer.sync()

        # Check if the methods were called with the correct arguments
        mock_endpoints.get_deployment_manager_client.assert_called_once()
        mock_get_deployment_manager_client.get_all_values_from_table.assert_called_once_with(
            DEPLOYMENT_MANAGER_RESOURCE_TABLE
        )
        mock_process_workflow.assert_called_once_with("workflow_id", "deployment_manager_config_json")

    @patch.object(DatastoreSyncer, "process_function_instance")
    @patch.object(DatastoreSyncer, "validate_deployment_manager_config")
    def test_process_workflow(self, mock_validate_deployment_manager_config, mock_process_function_instance):
        # Mocking the scenario where the process_workflow method is called successfully
        syncer = DatastoreSyncer()
        workflow_summary_instance = {"instance_summary": {}}
        syncer.initialize_workflow_summary_instance = MagicMock(return_value=workflow_summary_instance)
        syncer.get_last_synced_time = MagicMock(return_value=datetime(2022, 1, 1))
        syncer.endpoints.get_datastore_client().put_value_to_sort_key_table = MagicMock()

        deployment_manager_config_json = json.dumps(
            {"deployed_regions": json.dumps({"function_physical_instance": {"provider": "aws", "region": "us-east-1"}})}
        )

        mock_process_function_instance.return_value = 1

        # Call the method with test values
        syncer.process_workflow("workflow_id", deployment_manager_config_json)

        # Check that the validate_deployment_manager_config and process_function_instance methods were called
        mock_validate_deployment_manager_config.assert_called()
        mock_process_function_instance.assert_called()


if __name__ == "__main__":
    unittest.main()
