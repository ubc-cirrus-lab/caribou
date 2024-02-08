import unittest
from unittest.mock import Mock, patch
from multi_x_serverless.common.models.remote_client.remote_client import RemoteClient
from multi_x_serverless.data_collector.components.data_exporter import DataExporter
from multi_x_serverless.data_collector.components.provider.provider_exporter import ProviderExporter


class TestProviderExporter(unittest.TestCase):
    def setUp(self):
        self.client = Mock(spec=RemoteClient)
        self.provider_exporter = ProviderExporter(self.client, "provider_region_table", "provider_table")

    @patch.object(DataExporter, "_export_region_data")
    @patch.object(DataExporter, "_export_data")
    def test_export_all_data(self, mock_export_data, mock_export_region_data):
        provider_region_data = {"region1": {"Provider Region Data": "Data"}}
        provider_data = {"provider1": {"Provider Data": "Data"}}

        self.provider_exporter.export_all_data(provider_region_data, provider_data)

        mock_export_region_data.assert_called_once_with(provider_region_data)
        mock_export_data.assert_called_once_with("provider_table", provider_data, False)

    @patch.object(DataExporter, "_export_data")
    def test_export_available_region_table(self, mock_export_data):
        available_region_data = {"aws:region1": {"Region Specification Data": "Data"}}
        self.provider_exporter._available_region_table = "available_regions_table"

        self.provider_exporter.export_available_region_table(available_region_data)

        mock_export_data.assert_called_once_with("available_regions_table", available_region_data, True)


if __name__ == "__main__":
    unittest.main()
