import unittest
from unittest.mock import patch, MagicMock, mock_open
import os
import tempfile
import json
from caribou.deployment.client.remote_cli.remote_cli import (
    remove_remote_framework,
    deploy_remote_framework,
    valid_framework_dir,
    _retrieve_iam_trust_policy,
    _get_env_vars,
)
from caribou.common.constants import GLOBAL_SYSTEM_REGION
from caribou.common.models.remote_client.aws_remote_client import AWSRemoteClient
from caribou.deployment.common.config.config import Config
from caribou.deployment.common.deploy.deployment_packager import DeploymentPackager
from caribou.deployment.common.deploy.models.resource import Resource


class TestRemoteCLI(unittest.TestCase):
    @patch("caribou.deployment.client.remote_cli.remote_cli.AWSRemoteClient")
    @patch("caribou.deployment.client.remote_cli.remote_cli.get_all_available_timed_cli_functions")
    @patch("caribou.deployment.client.remote_cli.remote_cli.remove_aws_timers")
    @patch("caribou.deployment.client.remote_cli.remote_cli._is_aws_framework_deployed")
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
    @patch("caribou.deployment.client.remote_cli.remote_cli._is_aws_framework_deployed")
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
    @patch("caribou.deployment.client.remote_cli.remote_cli._is_aws_framework_deployed")
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


if __name__ == "__main__":
    unittest.main()
