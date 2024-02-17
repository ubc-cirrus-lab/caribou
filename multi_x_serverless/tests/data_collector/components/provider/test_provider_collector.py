import unittest
from unittest.mock import MagicMock, Mock, patch
from multi_x_serverless.data_collector.components.provider.provider_exporter import ProviderExporter
from multi_x_serverless.data_collector.components.provider.provider_retriever import ProviderRetriever
from multi_x_serverless.data_collector.components.provider.provider_collector import ProviderCollector


class TestProviderCollector(unittest.TestCase):
    def setUp(self):
        with patch(
            "multi_x_serverless.data_collector.components.provider.provider_retriever.ProviderRetriever"
        ) as mock_provider_retriever, patch(
            "multi_x_serverless.data_collector.components.provider.provider_exporter.ProviderExporter"
        ) as mock_provider_exporter:
            # Set up mock ProviderRetriever and ProviderExporter
            mock_provider_retriever.return_value = MagicMock()
            mock_provider_exporter.return_value = MagicMock()

            self.provider_collector = ProviderCollector()

    @patch.object(ProviderRetriever, "retrieve_available_regions")
    @patch.object(ProviderRetriever, "retrieve_provider_region_data")
    @patch.object(ProviderExporter, "export_available_region_table")
    @patch.object(ProviderExporter, "export_all_data")
    @patch.object(ProviderExporter, "get_modified_regions")
    @patch.object(ProviderExporter, "update_available_region_timestamp")
    def test_run(
        self,
        mock_update_available_region_timestamp,
        mock_get_modified_regions,
        mock_export_all_data,
        mock_export_available_region_table,
        mock_retrieve_provider_region_data,
        mock_retrieve_available_regions,
    ):
        mock_retrieve_available_regions.return_value = {"aws:region1": {"Region Specification Data": "Data"}}
        mock_retrieve_provider_region_data.return_value = {"aws:region1": {"Provider Region Data": "Data"}}
        mock_get_modified_regions.return_value = {"aws:region1"}

        self.provider_collector.run()

        mock_retrieve_available_regions.assert_called_once()
        mock_export_available_region_table.assert_called_once_with(
            {"aws:region1": {"Region Specification Data": "Data"}}
        )
        mock_retrieve_provider_region_data.assert_called_once()
        mock_export_all_data.assert_called_once_with({"aws:region1": {"Provider Region Data": "Data"}}, {})
        mock_get_modified_regions.assert_called_once()
        mock_update_available_region_timestamp.assert_called_once_with("provider_collector", {"aws:region1"})


if __name__ == "__main__":
    unittest.main()
