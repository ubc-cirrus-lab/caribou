import unittest
from unittest.mock import MagicMock, patch
from caribou.data_collector.components.workflow.workflow_exporter import WorkflowExporter
from caribou.data_collector.components.workflow.workflow_retriever import WorkflowRetriever
from caribou.data_collector.components.workflow.workflow_collector import WorkflowCollector


class TestWorkflowCollector(unittest.TestCase):
    @patch.object(WorkflowRetriever, "__init__", return_value=None)
    @patch.object(WorkflowExporter, "__init__", return_value=None)
    def setUp(self, mock_retriever_init, mock_exporter_init):
        self.collector = WorkflowCollector()
        self.collector._data_retriever = MagicMock(spec=WorkflowRetriever)
        self.collector._data_exporter = MagicMock(spec=WorkflowExporter)

    def test_init(self):
        self.assertEqual(self.collector._data_collector_name, "workflow_collector")
        self.assertIsInstance(self.collector._data_retriever, WorkflowRetriever)
        self.assertIsInstance(self.collector._data_exporter, WorkflowExporter)

    def test_run(self):
        mock_workflow_ids = ["workflow1", "workflow2"]
        self.collector._data_retriever.retrieve_all_workflow_ids.return_value = mock_workflow_ids
        self.collector.collect_single_workflow = MagicMock()

        self.collector.run()

        self.collector._data_retriever.retrieve_all_workflow_ids.assert_called_once()
        self.collector.collect_single_workflow.assert_any_call("workflow1")
        self.collector.collect_single_workflow.assert_any_call("workflow2")

    def test_collect_single_workflow(self):
        mock_workflow_summary_data = {"data": "data"}
        self.collector._data_retriever.retrieve_workflow_summary.return_value = mock_workflow_summary_data

        self.collector.collect_single_workflow("workflow1")

        self.collector._data_retriever.retrieve_available_regions.assert_called_once()
        self.collector._data_retriever.retrieve_workflow_summary.assert_called_once_with("workflow1")
        self.collector._data_exporter.export_all_data.assert_called_once_with({"workflow1": mock_workflow_summary_data})


if __name__ == "__main__":
    unittest.main()
