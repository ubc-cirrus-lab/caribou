import unittest
from unittest.mock import Mock, patch
from multi_x_serverless.deployment.client.multi_x_serverless_workflow import MultiXServerlessWorkflow


from multi_x_serverless.deployment.client.config import Config


class TestConfig(unittest.TestCase):
    def setUp(self):
        self.config = Config({"workflow_name": "test_workflow"}, "project_dir")

    def test_workflow_app(self):
        self.config.project_config["workflow_app"] = MultiXServerlessWorkflow("test_workflow")
        self.assertIsInstance(self.config.workflow_app, MultiXServerlessWorkflow)

    def test_workflow_name(self):
        self.assertEqual(self.config.workflow_name, "test_workflow")

    def test_python_version(self):
        self.assertTrue(self.config.python_version.startswith("python"))

    def test_environment_variables(self):
        self.config.project_config["environment_variables"] = [{"key": "ENV", "value": "test"}]
        self.assertEqual(self.config.environment_variables, {"ENV": "test"})

    def test_home_regions(self):
        self.config.project_config["home_regions"] = [["aws", "us-west-2"]]
        self.assertEqual(self.config.home_regions, [("aws", "us-west-2")])

    def test_home_regions_json(self):
        self.config.project_config["home_regions"] = [["aws", "us-west-2"]]
        self.assertEqual(self.config.home_regions_json, [["aws", "us-west-2"]])

    def test_estimated_invocations_per_month(self):
        self.config.project_config["estimated_invocations_per_month"] = 1000
        self.assertEqual(self.config.estimated_invocations_per_month, 1000)

    def test_constraints(self):
        self.config.project_config["constraints"] = {"memory": "512MB"}
        self.assertEqual(self.config.constraints, {"memory": "512MB"})

    def test_iam_policy_file(self):
        self.config.project_config["iam_policy_file"] = "policy.json"
        self.assertEqual(self.config.iam_policy_file, "policy.json")

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


if __name__ == "__main__":
    unittest.main()
