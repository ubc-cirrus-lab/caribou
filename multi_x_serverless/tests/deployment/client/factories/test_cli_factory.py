import unittest
from unittest.mock import Mock, patch
from pydantic import ValidationError

from multi_x_serverless.deployment.common.factories.deployer_factory import DeployerFactory


class TestCLIFactory(unittest.TestCase):
    @patch("sys.path")
    @patch("importlib.import_module")
    def test_load_workflow_app(self, mock_import, mock_sys):
        mock_import.return_value = Mock(spec=["workflow"])
        factory = DeployerFactory("project_dir")
        result = factory.load_workflow_app()
        self.assertIsNotNone(result)

    def test_validate_config(self):
        factory = DeployerFactory("project_dir")
        with self.assertRaises(RuntimeError):
            factory._validate_config("not a dictionary")


if __name__ == "__main__":
    unittest.main()
