import unittest
from unittest.mock import MagicMock
from caribou.common.models.remote_client.remote_client import RemoteClient
from caribou.data_collector.components.workflow.workflow_exporter import WorkflowExporter


class TestWorkflowExporter(unittest.TestCase):
    def setUp(self):
        self.client = MagicMock(spec=RemoteClient)
        self.exporter = WorkflowExporter(self.client, "workflow_instance_table")

    def test_init(self):
        self.assertEqual(self.exporter._client, self.client)
        self.assertEqual(self.exporter._workflow_summary_table, "workflow_instance_table")

    def test_export_all_data(self):
        workflow_summary_data = {"key1": {"data": "data1"}}
        self.exporter._export_workflow_summary = MagicMock()
        self.exporter.export_all_data(workflow_summary_data)
        self.exporter._export_workflow_summary.assert_called_once_with(workflow_summary_data)

    def test_export_workflow_summary(self):
        workflow_summary_data = {"key1": {"data": "data1"}}
        self.exporter._export_data = MagicMock()
        self.exporter._export_workflow_summary(workflow_summary_data)
        self.exporter._export_data.assert_called_once_with(
            "workflow_instance_table", workflow_summary_data, False, convert_to_bytes=True
        )


if __name__ == "__main__":
    unittest.main()
