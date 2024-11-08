import unittest
from unittest.mock import patch, MagicMock
from caribou.monitors.deployment_migrator import DeploymentMigrator


class TestDeploymentMigrator(unittest.TestCase):
    @patch("caribou.monitors.deployment_migrator.ReDeploymentServer")
    def test_check_local_deployment(self, mock_re_deployment_server):
        mock_client = MagicMock()
        mock_client.get_keys.return_value = ["workflow_1", "workflow_2"]
        mock_endpoints = MagicMock()
        mock_endpoints.get_deployment_algorithm_workflow_placement_decision_client.return_value = mock_client

        migrator = DeploymentMigrator(deployed_remotely=False)
        migrator._endpoints = mock_endpoints
        migrator.check()

        mock_re_deployment_server.assert_any_call("workflow_1")
        mock_re_deployment_server.assert_any_call("workflow_2")
        self.assertEqual(mock_re_deployment_server.call_count, 2)

    def test_check_remote_deployment(self):
        mock_client = MagicMock()
        mock_client.get_keys.return_value = ["workflow_1", "workflow_2"]

        mock_endpoints = MagicMock()
        mock_endpoints.get_deployment_algorithm_workflow_placement_decision_client.return_value = mock_client

        mock_remote_client = MagicMock()
        mock_endpoints.get_framework_cli_remote_client.return_value = mock_remote_client

        migrator = DeploymentMigrator(deployed_remotely=True)
        migrator._endpoints = mock_endpoints
        migrator.check()

        mock_remote_client.invoke_remote_framework_internal_action.assert_any_call(
            "re_deploy_workflow", {"workflow_id": "workflow_1"}
        )
        mock_remote_client.invoke_remote_framework_internal_action.assert_any_call(
            "re_deploy_workflow", {"workflow_id": "workflow_2"}
        )
        self.assertEqual(mock_remote_client.invoke_remote_framework_internal_action.call_count, 2)

    def test_remote_re_deploy_workflow(self):
        mock_remote_client = MagicMock()
        mock_endpoints = MagicMock()
        mock_endpoints.get_framework_cli_remote_client.return_value = mock_remote_client

        migrator = DeploymentMigrator(deployed_remotely=True)
        migrator._endpoints = mock_endpoints
        workflow_id = "workflow_1"
        migrator.remote_re_deploy_workflow(workflow_id)

        mock_remote_client.invoke_remote_framework_internal_action.assert_called_once_with(
            "re_deploy_workflow", {"workflow_id": workflow_id}
        )

    @patch("caribou.monitors.deployment_migrator.ReDeploymentServer")
    def test_re_deploy_workflow(self, mock_re_deployment_server):
        workflow_id = "workflow_1"
        migrator = DeploymentMigrator(deployed_remotely=False)
        migrator.re_deploy_workflow(workflow_id)

        mock_re_deployment_server.assert_called_once_with(workflow_id)
        mock_re_deployment_server_instance = mock_re_deployment_server.return_value
        mock_re_deployment_server_instance.run.assert_called_once()


if __name__ == "__main__":
    unittest.main()
