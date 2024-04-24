import unittest
from unittest.mock import patch, MagicMock
from caribou.monitors.deployment_migrator import DeploymentMigrator
from caribou.common.constants import WORKFLOW_PLACEMENT_SOLVER_STAGING_AREA_TABLE
from caribou.deployment.server.re_deployment_server import ReDeploymentServer


class TestDeploymentMigrator(unittest.TestCase):
    def setUp(self):
        self.function_deployment_monitor = DeploymentMigrator()
        self.mock_endpoints = MagicMock()
        self.function_deployment_monitor._endpoints = self.mock_endpoints

    @patch("caribou.monitors.deployment_migrator.ReDeploymentServer")
    def test_check(self, mock_re_deployment_server):
        # Arrange
        mock_client = MagicMock()
        self.mock_endpoints.get_deployment_algorithm_workflow_placement_decision_client.return_value = mock_client
        mock_client.get_keys.return_value = ["workflow1"]
        mock_re_deployment_server_instance = mock_re_deployment_server.return_value
        mock_re_deployment_server_instance.run.return_value = None

        # Act
        self.function_deployment_monitor.check()

        # Assert
        self.mock_endpoints.get_deployment_algorithm_workflow_placement_decision_client.assert_called_once()
        mock_client.get_keys.assert_called_once_with(WORKFLOW_PLACEMENT_SOLVER_STAGING_AREA_TABLE)
        mock_re_deployment_server.assert_called_once_with("workflow1")
        mock_re_deployment_server_instance.run.assert_called_once()


if __name__ == "__main__":
    unittest.main()
