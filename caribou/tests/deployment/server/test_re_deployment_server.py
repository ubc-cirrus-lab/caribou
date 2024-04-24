import unittest
import json
from unittest.mock import patch, MagicMock
from caribou.deployment.server.re_deployment_server import ReDeploymentServer
from caribou.common.constants import (
    DEPLOYMENT_MANAGER_RESOURCE_TABLE,
    WORKFLOW_PLACEMENT_DECISION_TABLE,
    WORKFLOW_PLACEMENT_SOLVER_STAGING_AREA_TABLE,
    DEPLOYMENT_OPTIMIZATION_MONITOR_RESOURCE_TABLE,
)


class TestReDeploymentServer(unittest.TestCase):
    @patch("caribou.deployment.server.re_deployment_server.Endpoints")
    def setUp(self, mock_endpoints):
        # Set up the mock
        mock_client = MagicMock()
        mock_client.get_value_from_table.return_value = json.dumps({"key": "value"})
        mock_endpoints.return_value.get_deployment_manager_client.return_value = mock_client

        self.mock_endpoints = mock_endpoints

        # Call the method
        self.re_deployment_server = ReDeploymentServer("workflow_id")

    def test_init(self):
        # Check that the _workflow_data attribute was initialized correctly
        self.assertEqual(self.re_deployment_server._workflow_data, {"key": "value"})

        # Check that the mock was called with the correct arguments
        self.mock_endpoints.return_value.get_deployment_manager_client.return_value.get_value_from_table.assert_called_once_with(
            DEPLOYMENT_MANAGER_RESOURCE_TABLE, "workflow_id"
        )

    def test_load_workflow_data(self):
        # Set up the mock
        mock_client = MagicMock()
        self.mock_endpoints.return_value.get_deployment_manager_client.return_value = mock_client
        mock_client.get_value_from_table.return_value = json.dumps({"key": "value"})

        # Call the method
        result = self.re_deployment_server._load_workflow_data()

        # Check that the result is correct
        self.assertEqual(result, {"key": "value"})

        # Check that the mock was called with the correct arguments
        mock_client.get_value_from_table.assert_called_once_with(DEPLOYMENT_MANAGER_RESOURCE_TABLE, "workflow_id")

    @patch.object(ReDeploymentServer, "_run_deployer")
    @patch.object(ReDeploymentServer, "_update_workflow_placement_decision")
    def test_run(self, mock_update_workflow_placement_decision, mock_run_deployer):
        # Set up the mock
        mock_client = MagicMock()
        mock_client.get_value_from_table.return_value = json.dumps(
            {
                "time_keys_to_staging_area_data": {
                    "time_key1": "specific_staging_area_data1",
                    "time_key2": "specific_staging_area_data2",
                },
                "expiry_time": "expiry_time",
            }
        )
        self.mock_endpoints.return_value.get_deployment_optimization_monitor_client.return_value = mock_client

        self.re_deployment_server._workflow_data = {
            "workflow_function_descriptions": json.dumps(["description1", "description2"]),
            "deployment_config": json.dumps({"key": "value"}),
            "deployed_regions": json.dumps({"region1": "deployed1", "region2": "deployed2"}),
        }

        # Call the method
        self.re_deployment_server.run()

        # Check that the mocks were called with the correct arguments
        mock_run_deployer.assert_any_call(
            deployment_config={"key": "value"},
            workflow_function_descriptions=["description1", "description2"],
            deployed_regions={"region1": "deployed1", "region2": "deployed2"},
            specific_staging_area_data="specific_staging_area_data1",
            time_key="time_key1",
        )
        mock_run_deployer.assert_any_call(
            deployment_config={"key": "value"},
            workflow_function_descriptions=["description1", "description2"],
            deployed_regions={"region1": "deployed1", "region2": "deployed2"},
            specific_staging_area_data="specific_staging_area_data2",
            time_key="time_key2",
        )
        mock_update_workflow_placement_decision.assert_called_once_with("expiry_time")

    @patch("caribou.deployment.server.re_deployment_server.create_default_deployer")
    @patch("caribou.deployment.server.re_deployment_server.DeployerFactory")
    def test_run_deployer(self, mock_deployer_factory, mock_create_default_deployer):
        # Set up the mocks
        mock_deployer = MagicMock()
        mock_deployer.re_deploy.return_value = "new_deployment_instances"
        mock_create_default_deployer.return_value = mock_deployer

        mock_config = MagicMock()
        mock_deployer_factory.return_value.create_config_obj_from_dict.return_value = mock_config

        # Call the method
        self.re_deployment_server._run_deployer(
            deployment_config={"key": "value"},
            workflow_function_descriptions=["description1", "description2"],
            deployed_regions={"region1": "deployed1", "region2": "deployed2"},
            specific_staging_area_data={"key": "value"},
            time_key="time_key",
        )

        # Check that the mocks were called with the correct arguments
        mock_deployer_factory.return_value.create_config_obj_from_dict.assert_called_once_with(
            deployment_config={"key": "value"}
        )
        mock_create_default_deployer.assert_called_once_with(config=mock_config)
        mock_deployer.re_deploy.assert_called_once_with(
            workflow_function_descriptions=["description1", "description2"],
            deployed_regions={"region1": "deployed1", "region2": "deployed2"},
            specific_staging_area_data={"key": "value"},
        )

        # Check that the _time_keys_to_instances attribute was updated correctly
        self.assertEqual(self.re_deployment_server._time_keys_to_instances, {"time_key": "new_deployment_instances"})

    def test_update_workflow_placement_decision(self):
        # Set up the mock
        mock_client = MagicMock()
        mock_client.get_value_from_table.return_value = json.dumps({"workflow_placement": {}})
        self.mock_endpoints.return_value.get_deployment_optimization_monitor_client.return_value = mock_client

        self.re_deployment_server._time_keys_to_instances = {"time_key": "instance"}

        # Call the method
        self.re_deployment_server._update_workflow_placement_decision("expiry_time")

        # Check that the mocks were called with the correct arguments
        mock_client.get_value_from_table.assert_called_once_with(WORKFLOW_PLACEMENT_DECISION_TABLE, "workflow_id")
        mock_client.set_value_in_table.assert_called_once_with(
            WORKFLOW_PLACEMENT_DECISION_TABLE,
            "workflow_id",
            json.dumps(
                {
                    "workflow_placement": {
                        "current_deployment": {
                            "expiry_time": "expiry_time",
                            "time_keys": ["time_key"],
                            "instances": {"time_key": "instance"},
                        },
                    },
                }
            ),
        )
        mock_client.remove_key.assert_called_once_with(WORKFLOW_PLACEMENT_SOLVER_STAGING_AREA_TABLE, "workflow_id")

    def test_upload_new_deployed_regions(self):
        # Set up the mock
        mock_client = MagicMock()
        self.mock_endpoints.return_value.get_deployment_manager_client.return_value = mock_client

        self.re_deployment_server._workflow_data = {"key": "value"}

        # Call the method
        self.re_deployment_server._upload_new_deployed_regions({"region1": "deployed1", "region2": "deployed2"})

        # Check that the mock was called with the correct arguments
        mock_client.set_value_in_table.assert_called_once_with(
            DEPLOYMENT_MANAGER_RESOURCE_TABLE,
            "workflow_id",
            json.dumps(
                {
                    "key": "value",
                    "deployed_regions": json.dumps({"region1": "deployed1", "region2": "deployed2"}),
                }
            ),
        )

    def test_check_workflow_already_deployed(self):
        # Set up the mock
        mock_client = MagicMock()
        mock_client.get_key_present_in_table.return_value = True
        self.mock_endpoints.return_value.get_deployment_optimization_monitor_client.return_value = mock_client

        # Call the method and check that it does not raise an exception
        self.re_deployment_server._check_workflow_already_deployed()

        # Check that the mock was called with the correct arguments
        mock_client.get_key_present_in_table.assert_called_once_with(
            DEPLOYMENT_OPTIMIZATION_MONITOR_RESOURCE_TABLE, "workflow_id"
        )

        # Set up the mock to return False
        mock_client.get_key_present_in_table.return_value = False

        # Call the method and check that it raises an exception
        with self.assertRaises(RuntimeError):
            self.re_deployment_server._check_workflow_already_deployed()


if __name__ == "__main__":
    unittest.main()
