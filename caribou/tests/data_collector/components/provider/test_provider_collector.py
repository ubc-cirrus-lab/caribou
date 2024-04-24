import unittest
from unittest.mock import MagicMock, Mock, patch
from caribou.data_collector.components.provider.provider_exporter import ProviderExporter
from caribou.data_collector.components.provider.provider_retriever import ProviderRetriever
from caribou.data_collector.components.provider.provider_collector import ProviderCollector


class TestProviderCollector(unittest.TestCase):
    def setUp(self):
        with patch("os.environ.get") as mock_os_environ_get, patch("boto3.client") as mock_boto3, patch(
            "caribou.common.utils.str_to_bool"
        ) as mock_str_to_bool:
            mock_boto3.return_value = MagicMock()
            mock_os_environ_get.return_value = "test_key"
            mock_str_to_bool.return_value = False

            # Need to do the above as issue with Provider Retriever constructor
            # Maybe a better way to do this is warrented
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
