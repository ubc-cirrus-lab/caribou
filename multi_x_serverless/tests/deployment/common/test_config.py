import unittest
from unittest.mock import patch
from multi_x_serverless.deployment.client.multi_x_serverless_workflow import MultiXServerlessWorkflow
from collections import namedtuple


from multi_x_serverless.deployment.common.config import Config


class TestConfig(unittest.TestCase):
    def setUp(self):
        self.config = Config({"workflow_name": "test_workflow"}, "project_dir")

    def test_workflow_app(self):
        self.config.project_config["workflow_app"] = MultiXServerlessWorkflow("test_workflow")
        self.assertIsInstance(self.config.workflow_app, MultiXServerlessWorkflow)

    def test_workflow_name(self):
        self.assertEqual(self.config.workflow_name, "test_workflow")

    @patch("sys.version_info", namedtuple("version_info", ["major", "minor"])(2, 7))
    def test_python_version_2(self):
        self.assertEqual(self.config.python_version, "python2.7")

    @patch("sys.version_info", namedtuple("version_info", ["major", "minor"])(3, 6))
    def test_python_version_3_6(self):
        self.assertEqual(self.config.python_version, "python3.6")

    @patch("sys.version_info", namedtuple("version_info", ["major", "minor"])(3, 10))
    def test_python_version_3_10(self):
        self.assertEqual(self.config.python_version, "python3.10")

    @patch("sys.version_info", namedtuple("version_info", ["major", "minor"])(3, 11))
    def test_python_version_3_11(self):
        self.assertEqual(self.config.python_version, "python3.11")

    def test_environment_variables(self):
        self.config.project_config["environment_variables"] = [{"key": "ENV", "value": "test"}]
        self.assertEqual(self.config.environment_variables, {"ENV": "test"})

    def test_environment_variables_none(self):
        with patch.object(self.config, "_lookup", return_value=None):
            self.assertEqual(self.config.environment_variables, {})

    def test_environment_variables_invalid_value_type(self):
        self.config.project_config["environment_variables"] = [{"key": "ENV", "value": 123}]
        with self.assertRaises(RuntimeError, msg="Environment variable value need to be a str"):
            _ = self.config.environment_variables

    def test_environment_variables_invalid_key_type(self):
        self.config.project_config["environment_variables"] = [{"key": 123, "value": "test"}]
        with self.assertRaises(RuntimeError, msg="Environment variable key need to be a str"):
            _ = self.config.environment_variables

    def test_home_regions(self):
        self.config.project_config["home_regions"] = [{"provider": "aws", "region": "us-west-2"}]
        self.assertEqual(self.config.home_regions, [{"provider": "aws", "region": "us-west-2"}])

    def test_estimated_invocations_per_month(self):
        self.config.project_config["estimated_invocations_per_month"] = 1000
        self.assertEqual(self.config.estimated_invocations_per_month, 1000)

    def test_constraints(self):
        self.config.project_config["constraints"] = {"memory": "512MB"}
        self.assertEqual(self.config.constraints, {"memory": "512MB"})

    def test_iam_policy_file(self):
        self.config.project_config["iam_policy_file"] = "policy.json"
        self.assertEqual(self.config.iam_policy_file, "policy.json")

    def test_regions_and_providers(self):
        self.config.project_config["regions_and_providers"] = {
            "only_regions": [
                {
                    "provider": "aws",
                    "region": "us-east-1",
                }
            ],
            "forbidden_regions": [
                {
                    "provider": "aws",
                    "region": "us-east-2",
                }
            ],
            "providers": {
                "aws": {
                    "config": {
                        "timeout": 60,
                        "memory": 128,
                    },
                },
            },
        }
        self.assertEqual(
            self.config.regions_and_providers,
            {
                "only_regions": [
                    {
                        "provider": "aws",
                        "region": "us-east-1",
                    }
                ],
                "forbidden_regions": [
                    {
                        "provider": "aws",
                        "region": "us-east-2",
                    }
                ],
                "providers": {
                    "aws": {
                        "config": {
                            "timeout": 60,
                            "memory": 128,
                        },
                    },
                },
            },
        )

    @patch("os.path.join")
    @patch("builtins.open")
    @patch("yaml.safe_load")
    def test_deployed_resources(self, mock_yaml, mock_open, mock_os):
        mock_yaml.return_value = {"resources": ["resource1", "resource2"]}
        resources = self.config.deployed_resources()
        self.assertEqual(resources, ["resource1", "resource2"])

    @patch("os.path.join")
    @patch("builtins.open")
    @patch("yaml.dump")
    def test_update_deployed_resources(self, mock_yaml, mock_open, mock_os):
        self.config.update_deployed_resources(["resource1", "resource2"])
        mock_yaml.assert_called_once_with(
            {"resources": ["resource1", "resource2"]}, mock_open.return_value.__enter__.return_value
        )

    @patch("os.path.join")
    @patch("builtins.open")
    @patch("yaml.safe_load")
    def test_deployed_resources_none(self, mock_yaml, mock_open, mock_os):
        mock_yaml.return_value = None
        resources = self.config.deployed_resources()
        self.assertEqual(resources, [])

    @patch.object(Config, "_lookup", return_value=None)
    def test_workflow_app_lookup_failure(self, mock_lookup):
        instance = Config({"workflow_name": "test_workflow"}, "project_dir")
        with self.assertRaises(RuntimeError, msg="workflow_app must be a Workflow instance"):
            _ = self.config.workflow_app
        mock_lookup.assert_called_once_with("workflow_app")


if __name__ == "__main__":
    unittest.main()
