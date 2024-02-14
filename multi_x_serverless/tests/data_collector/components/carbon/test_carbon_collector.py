import unittest
from unittest.mock import Mock, patch
import os
from multi_x_serverless.data_collector.components.carbon.carbon_exporter import CarbonExporter
from multi_x_serverless.data_collector.components.carbon.carbon_retriever import CarbonRetriever
from multi_x_serverless.data_collector.components.carbon.carbon_collector import CarbonCollector


class TestCarbonCollector(unittest.TestCase):
    def setUp(self):
        self.config = {"carbon_transmission_cost_calculator": "distance"}
        with patch("os.environ.get") as mock_os_environ_get:
            mock_os_environ_get.return_value = "mock_token"
            self.carbon_collector = CarbonCollector()

    @patch.object(CarbonRetriever, "retrieve_available_regions")
    @patch.object(CarbonRetriever, "retrieve_carbon_region_data")
    @patch.object(CarbonExporter, "export_all_data")
    @patch.object(CarbonExporter, "get_modified_regions")
    @patch.object(CarbonExporter, "update_available_region_timestamp")
    def test_run(
        self,
        mock_update_available_region_timestamp,
        mock_get_modified_regions,
        mock_export_all_data,
        mock_retrieve_carbon_region_data,
        mock_retrieve_available_regions,
    ):
        mock_retrieve_available_regions.return_value = {"aws:region1": {"latitude": 1.0, "longitude": 1.0}}
        mock_retrieve_carbon_region_data.return_value = {"aws:region1": {"carbon_intensity": 100, "unit": "gCO2eq/kWh"}}
        mock_get_modified_regions.return_value = {"aws:region1"}

        self.carbon_collector.run()

        mock_retrieve_available_regions.assert_called_once()
        mock_retrieve_carbon_region_data.assert_called_once()
        mock_export_all_data.assert_called_once_with({"aws:region1": {"carbon_intensity": 100, "unit": "gCO2eq/kWh"}})
        mock_get_modified_regions.assert_called_once()
        mock_update_available_region_timestamp.assert_called_once_with("carbon_collector", {"aws:region1"})


if __name__ == "__main__":
    unittest.main()
