import unittest
from unittest.mock import Mock, patch
from multi_x_serverless.data_collector.components.performance.performance_exporter import PerformanceExporter
from multi_x_serverless.data_collector.components.performance.performance_retriever import PerformanceRetriever
from multi_x_serverless.data_collector.components.performance.performance_collector import PerformanceCollector


class TestPerformanceCollector(unittest.TestCase):
    def setUp(self):
        self.mock_client = Mock()
        self.performance_collector = PerformanceCollector()

    @patch.object(PerformanceRetriever, "retrieve_available_regions")
    @patch.object(PerformanceRetriever, "retrieve_runtime_region_data")
    @patch.object(PerformanceExporter, "export_all_data")
    @patch.object(PerformanceExporter, "get_modified_regions")
    @patch.object(PerformanceExporter, "update_available_region_timestamp")
    def test_run(
        self,
        mock_update_timestamp,
        mock_get_regions,
        mock_export_data,
        mock_retrieve_runtime_data,
        mock_retrieve_regions,
    ):
        mock_retrieve_regions.return_value = {"aws:region1": "data1", "aws:region2": "data2"}
        mock_retrieve_runtime_data.return_value = {"aws:region1": "data1", "aws:region2": "data2"}
        mock_get_regions.return_value = {"aws:region1", "aws:region2"}

        self.performance_collector.run()

        mock_retrieve_regions.assert_called_once()
        mock_retrieve_runtime_data.assert_called_once()
        mock_export_data.assert_called_once_with(mock_retrieve_runtime_data.return_value)
        mock_get_regions.assert_called_once()
        mock_update_timestamp.assert_called_once_with("performance_collector", mock_get_regions.return_value)


if __name__ == "__main__":
    unittest.main()
