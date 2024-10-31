import unittest
from unittest.mock import patch

from caribou.deployment.client.remote_cli.remote_cli_handler import caribou_cli
from caribou.deployment.client import __version__ as CARIBOU_VERSION


class TestRemoteCLIHandler(unittest.TestCase):
    def setUp(self):
        self.event = {
            "action": "",
            "workflow_id": "test_workflow_id",
            "collector": "provider",
            "deployment_metrics_calculator_type": "simple",
            "type": "check_workflow",
            "event": {
                "workflow_id": "test_workflow_id",
                "deployment_metrics_calculator_type": "simple",
                "solve_hours": ["1", "2"],
                "leftover_tokens": 10,
            },
        }
        self.context = {}

    def test_caribou_cli_no_action(self):
        self.event.pop("action")
        response = caribou_cli(self.event, self.context)
        self.assertEqual(response, {"status": 400, "message": "No action specified"})

    def test_caribou_cli_handle_run_deployment_migrator(self):
        self.event["action"] = "run_deployment_migrator"
        with patch("caribou.deployment.client.remote_cli.remote_cli_handler.DeploymentMigrator") as MockMigrator:
            mock_instance = MockMigrator.return_value
            response = caribou_cli(self.event, self.context)
            mock_instance.check.assert_called_once()
            self.assertEqual(response, {"status": 200, "message": "Deployment migrator started"})

    def test_caribou_cli_handle_remove_workflow(self):
        self.event["action"] = "remove"
        with patch("caribou.deployment.client.remote_cli.remote_cli_handler.Client") as MockClient:
            mock_instance = MockClient.return_value
            response = caribou_cli(self.event, self.context)
            mock_instance.remove.assert_called_once()
            self.assertEqual(response, {"status": 200, "message": "Workflow test_workflow_id removal started"})

    def test_caribou_cli_handle_manage_deployments(self):
        self.event["action"] = "manage_deployments"
        with patch("caribou.deployment.client.remote_cli.remote_cli_handler.DeploymentManager") as MockManager:
            mock_instance = MockManager.return_value
            response = caribou_cli(self.event, self.context)
            mock_instance.check.assert_called_once()
            self.assertEqual(response, {"status": 200, "message": "Deployment check started, using simple calculator"})

    def test_caribou_cli_handle_log_sync(self):
        self.event["action"] = "log_sync"
        with patch("caribou.deployment.client.remote_cli.remote_cli_handler.LogSyncer") as MockSyncer:
            mock_instance = MockSyncer.return_value
            response = caribou_cli(self.event, self.context)
            mock_instance.sync.assert_called_once()
            self.assertEqual(response, {"status": 200, "message": "Log sync started"})

    def test_caribou_cli_handle_data_collect(self):
        self.event["action"] = "data_collect"
        with patch("caribou.deployment.client.remote_cli.remote_cli_handler.ProviderCollector") as MockCollector:
            mock_instance = MockCollector.return_value
            response = caribou_cli(self.event, self.context)
            mock_instance.run.assert_called_once()
            self.assertEqual(
                response, {"status": 200, "scheduled_collector": "provider", "workflow_id": "test_workflow_id"}
            )

    def test_caribou_cli_handle_list_caribou_version(self):
        self.event["action"] = "version"
        response = caribou_cli(self.event, self.context)
        self.assertEqual(response, {"status": 200, "version": CARIBOU_VERSION})

    def test_caribou_cli_handle_list_workflows(self):
        self.event["action"] = "list"
        with patch("caribou.deployment.client.remote_cli.remote_cli_handler.Client") as MockClient:
            mock_instance = MockClient.return_value
            mock_instance.list_workflows.return_value = ["workflow1", "workflow2"]
            response = caribou_cli(self.event, self.context)
            mock_instance.list_workflows.assert_called_once()
            self.assertEqual(response, {"status": 200, "workflows": ["workflow1", "workflow2"]})

    def test_caribou_cli_handle_run(self):
        self.event["action"] = "run"
        with patch("caribou.deployment.client.remote_cli.remote_cli_handler.Client") as MockClient:
            mock_instance = MockClient.return_value
            mock_instance.run.return_value = "run_id"
            response = caribou_cli(self.event, self.context)
            mock_instance.run.assert_called_once()
            self.assertEqual(response, {"status": 200, "run_id": "run_id"})

    def test_caribou_cli_handle_internal_action(self):
        self.event["action"] = "internal_action"
        with patch("caribou.deployment.client.remote_cli.remote_cli_handler.handle_internal_action") as MockHandler:
            caribou_cli(self.event, self.context)
            MockHandler.assert_called_once_with(self.event)

    def test_handle_internal_action_no_type(self):
        self.event["action"] = "internal_action"
        self.event.pop("type")
        response = caribou_cli(self.event, self.context)
        self.assertEqual(response, {"status": 400, "message": "No special_action specified"})

    def test_handle_internal_action_unknown_type(self):
        self.event["action"] = "internal_action"
        self.event["type"] = "unknown_type"
        response = caribou_cli(self.event, self.context)
        self.assertEqual(response, {"status": 400, "message": "Unknown special action"})


if __name__ == "__main__":
    unittest.main()
