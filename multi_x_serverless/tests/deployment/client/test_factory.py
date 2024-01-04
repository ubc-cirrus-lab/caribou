import unittest
from unittest.mock import Mock, patch
from pydantic import ValidationError

from multi_x_serverless.deployment.client.factory import CLIFactory


class TestCLIFactory(unittest.TestCase):
    @patch("os.path.join")
    @patch("builtins.open")
    @patch("yaml.safe_load")
    def test_load_project_config(self, mock_yaml, mock_open, mock_os):
        mock_yaml.return_value = {"key": "value"}
        factory = CLIFactory("project_dir")
        result = factory.load_project_config()
        self.assertEqual(result, {"key": "value"})

    @patch("sys.path")
    @patch("importlib.import_module")
    def test_load_workflow_app(self, mock_import, mock_sys):
        mock_import.return_value = Mock(spec=["workflow"])
        factory = CLIFactory("project_dir")
        result = factory.load_workflow_app()
        self.assertIsNotNone(result)

    def test_validate_config(self):
        factory = CLIFactory("project_dir")
        with self.assertRaises(RuntimeError):
            factory._validate_config("not a dictionary")

    def test_create_session(self):
        factory = CLIFactory("project_dir")
        result = factory.create_session()
        self.assertIsNotNone(result)


if __name__ == "__main__":
    unittest.main()
