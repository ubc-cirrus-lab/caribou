import unittest
from unittest.mock import patch, mock_open
import os
import tempfile
from caribou.deployment.client.remote_cli.remote_cli import (
    _get_timer_rule_name,
    action_type_to_function_name,
    get_all_available_timed_cli_functions,
    get_all_default_timed_cli_functions,
    get_cli_invoke_payload,
    is_aws_framework_deployed,
    remove_aws_timers,
    remove_remote_framework,
    deploy_remote_framework,
    report_timer_schedule_expression,
    setup_aws_timers,
    valid_framework_dir,
    _retrieve_iam_trust_policy,
    _get_env_vars,
)


class TestRemoteCLI(unittest.TestCase):
    @patch("caribou.deployment.client.remote_cli.remote_cli.AWSRemoteClient")
    @patch("caribou.deployment.client.remote_cli.remote_cli.get_all_available_timed_cli_functions")
    @patch("caribou.deployment.client.remote_cli.remote_cli.remove_aws_timers")
    @patch("caribou.deployment.client.remote_cli.remote_cli.is_aws_framework_deployed")
    def test_remove_remote_framework(
        self, mock_is_deployed, mock_remove_timers, mock_get_functions, MockAWSRemoteClient
    ):
        mock_client = MockAWSRemoteClient.return_value
        mock_client.resource_exists.side_effect = [True, True, True]
        mock_is_deployed.return_value = True
        mock_get_functions.return_value = ["provider_collector", "carbon_collector"]

        remove_remote_framework()

        mock_remove_timers.assert_called_once_with(["provider_collector", "carbon_collector"], verbose=True)
        mock_client.remove_role.assert_called_once_with("caribou_deployment_policy")
        mock_client.remove_function.assert_called_once_with("caribou_cli")
        mock_client.remove_ecr_repository.assert_called_once_with("caribou_cli")

    @patch("caribou.deployment.client.remote_cli.remote_cli.AWSRemoteClient")
    @patch("caribou.deployment.client.remote_cli.remote_cli.get_all_available_timed_cli_functions")
    @patch("caribou.deployment.client.remote_cli.remote_cli.remove_aws_timers")
    @patch("caribou.deployment.client.remote_cli.remote_cli.is_aws_framework_deployed")
    def test_remove_remote_framework_not_deployed(
        self, mock_is_deployed, mock_remove_timers, mock_get_functions, MockAWSRemoteClient
    ):
        mock_client = MockAWSRemoteClient.return_value
        mock_client.resource_exists.side_effect = [True, True, True]
        mock_is_deployed.return_value = False
        mock_get_functions.return_value = ["provider_collector", "carbon_collector"]

        remove_remote_framework()

        mock_remove_timers.assert_called_once_with(["provider_collector", "carbon_collector"], verbose=False)
        mock_client.remove_role.assert_called_once_with("caribou_deployment_policy")
        mock_client.remove_function.assert_called_once_with("caribou_cli")
        mock_client.remove_ecr_repository.assert_called_once_with("caribou_cli")

    @patch("caribou.deployment.client.remote_cli.remote_cli.AWSRemoteClient")
    @patch("caribou.deployment.client.remote_cli.remote_cli.get_all_available_timed_cli_functions")
    @patch("caribou.deployment.client.remote_cli.remote_cli.remove_aws_timers")
    @patch("caribou.deployment.client.remote_cli.remote_cli.is_aws_framework_deployed")
    def test_remove_remote_framework_no_resources(
        self, mock_is_deployed, mock_remove_timers, mock_get_functions, MockAWSRemoteClient
    ):
        mock_client = MockAWSRemoteClient.return_value
        mock_client.resource_exists.side_effect = [False, False, False]
        mock_is_deployed.return_value = True
        mock_get_functions.return_value = ["provider_collector", "carbon_collector"]

        remove_remote_framework()

        mock_remove_timers.assert_called_once_with(["provider_collector", "carbon_collector"], verbose=True)
        mock_client.remove_role.assert_not_called()
        mock_client.remove_function.assert_not_called()
        mock_client.remove_ecr_repository.assert_not_called()

    @patch("caribou.deployment.client.remote_cli.remote_cli.AWSRemoteClient")
    @patch("caribou.deployment.client.remote_cli.remote_cli.DeploymentPackager")
    @patch("builtins.open", new_callable=mock_open, read_data='{"aws": {}}')
    @patch("tempfile.TemporaryDirectory", return_value=tempfile.TemporaryDirectory())
    def test_deploy_aws_framework(self, mock_tempdir, mock_open, MockDeploymentPackager, MockAWSRemoteClient):
        mock_client = MockAWSRemoteClient.return_value
        mock_packager = MockDeploymentPackager.return_value
        mock_client.resource_exists.side_effect = [True, True, False]
        mock_packager.create_framework_package.return_value = "/fake/path/to/zip"

        with patch.dict(os.environ, {"GOOGLE_API_KEY": "fake_key", "ELECTRICITY_MAPS_AUTH_TOKEN": "fake_token"}):
            deploy_remote_framework("/fake/project/dir", 300, 128, 512)

        mock_client.remove_role.assert_called_once_with("caribou_deployment_policy")
        mock_client.remove_function.assert_called_once_with("caribou_cli")
        mock_client.create_role.assert_called_once()
        mock_client.deploy_remote_cli.assert_called_once()

    @patch(
        "os.path.exists",
        side_effect=lambda x: x
        in ["/fake/project/dir/caribou", "/fake/project/dir/caribou-go", "/fake/project/dir/pyproject.toml"],
    )
    def test_valid_framework_dir(self, mock_exists):
        self.assertTrue(valid_framework_dir("/fake/project/dir"))
        self.assertFalse(valid_framework_dir("/invalid/project/dir"))

    def test_retrieve_iam_trust_policy(self):
        expected_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "",
                    "Effect": "Allow",
                    "Principal": {"Service": ["lambda.amazonaws.com", "states.amazonaws.com"]},
                    "Action": "sts:AssumeRole",
                }
            ],
        }
        self.assertEqual(_retrieve_iam_trust_policy(), expected_policy)

    def test_get_env_vars(self):
        with patch.dict(os.environ, {"GOOGLE_API_KEY": "fake_key", "ELECTRICITY_MAPS_AUTH_TOKEN": "fake_token"}):
            env_vars = _get_env_vars(["GOOGLE_API_KEY", "ELECTRICITY_MAPS_AUTH_TOKEN"])
            self.assertEqual(env_vars, {"GOOGLE_API_KEY": "fake_key", "ELECTRICITY_MAPS_AUTH_TOKEN": "fake_token"})

        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(EnvironmentError):
                _get_env_vars(["GOOGLE_API_KEY", "ELECTRICITY_MAPS_AUTH_TOKEN"])

    def test_get_all_available_timed_cli_functions(self):
        expected_functions = [
            "provider_collector",
            "carbon_collector",
            "performance_collector",
            "log_syncer",
            "deployment_manager",
            "deployment_migrator",
        ]
        self.assertEqual(get_all_available_timed_cli_functions(), expected_functions)

    def test_get_all_default_timed_cli_functions(self):
        expected_schedules = {
            "provider_collector": "cron(5 0 1 * ? *)",
            "carbon_collector": "cron(30 0 * * ? *)",
            "performance_collector": "cron(30 0 * * ? *)",
            "log_syncer": "cron(5 0 * * ? *)",
            "deployment_manager": "cron(0 1 * * ? *)",
            "deployment_migrator": "cron(0 2 * * ? *)",
        }
        self.assertEqual(get_all_default_timed_cli_functions(), expected_schedules)

    @patch("caribou.deployment.client.remote_cli.remote_cli.AWSRemoteClient")
    def test_is_aws_framework_deployed(self, MockAWSRemoteClient):
        mock_client = MockAWSRemoteClient.return_value
        mock_client.resource_exists.side_effect = [True, True, True]
        self.assertTrue(is_aws_framework_deployed(mock_client))

        mock_client.resource_exists.side_effect = [False, True, True]
        self.assertFalse(is_aws_framework_deployed(mock_client))

    def test_get_timer_rule_name(self):
        self.assertEqual(_get_timer_rule_name("test_function"), "test_function-timer-rule")

    def test_get_cli_invoke_payload(self):
        expected_payload = {
            "action": "data_collect",
            "collector": "provider",
        }
        self.assertEqual(get_cli_invoke_payload("provider_collector"), expected_payload)

    def test_action_type_to_function_name(self):
        self.assertEqual(action_type_to_function_name("log_sync"), "log_syncer")
        with self.assertRaises(ValueError):
            action_type_to_function_name("invalid_action")

    @patch("caribou.deployment.client.remote_cli.remote_cli.AWSRemoteClient")
    @patch("caribou.deployment.client.remote_cli.remote_cli.get_description")
    def test_setup_aws_timers(self, mock_get_description, MockAWSRemoteClient):
        mock_client = MockAWSRemoteClient.return_value
        mock_client.resource_exists.return_value = True
        mock_get_description.return_value = "Every day at 12:30 AM"

        new_rules = [("carbon_collector", "cron(30 0 * * ? *)")]
        setup_aws_timers(new_rules)

        mock_client.create_timer_rule.assert_called_once()
        mock_get_description.assert_called_once()

    @patch("caribou.deployment.client.remote_cli.remote_cli.AWSRemoteClient")
    def test_remove_aws_timers(self, MockAWSRemoteClient):
        mock_client = MockAWSRemoteClient.return_value
        mock_client.resource_exists.return_value = True

        remove_aws_timers(["carbon_collector"])

        mock_client.remove_timer_rule.assert_called_once()

    @patch("caribou.deployment.client.remote_cli.remote_cli.AWSRemoteClient")
    def test_report_timer_schedule_expression(self, MockAWSRemoteClient):
        mock_client = MockAWSRemoteClient.return_value
        mock_client.get_timer_rule_schedule_expression.return_value = "cron(30 0 * * ? *)"

        self.assertEqual(report_timer_schedule_expression("carbon_collector"), "cron(30 0 * * ? *)")


if __name__ == "__main__":
    unittest.main()
