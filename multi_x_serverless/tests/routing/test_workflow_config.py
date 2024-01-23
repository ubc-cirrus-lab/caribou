import unittest
from unittest.mock import Mock, patch
from multi_x_serverless.routing.workflow_config import WorkflowConfig


class TestWorkflowConfig(unittest.TestCase):
    @patch("multi_x_serverless.routing.workflow_config.WorkflowConfigSchema")
    def setUp(self, mock_schema):
        self.mock_schema = mock_schema
        self.workflow_config = WorkflowConfig(
            {"workflow_name": "test", "workflow_version": "1.0", "workflow_id": "123"}
        )

    def test_verify(self):
        self.mock_schema.assert_called_once()

    def test_workflow_name(self):
        self.assertEqual(self.workflow_config.workflow_name, "test")

    def test_workflow_version(self):
        self.assertEqual(self.workflow_config.workflow_version, "1.0")

    def test_workflow_id(self):
        self.assertEqual(self.workflow_config.workflow_id, "123")


if __name__ == "__main__":
    unittest.main()
