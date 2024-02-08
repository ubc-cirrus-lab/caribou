import unittest
from unittest.mock import Mock, patch
from multi_x_serverless.data_collector.components.data_exporter import DataExporter
from multi_x_serverless.data_collector.components.carbon.carbon_exporter import CarbonExporter


class TestCarbonExporter(unittest.TestCase):
    def setUp(self):
        self.mock_client = Mock()
        self.carbon_exporter = CarbonExporter(self.mock_client, "performance_region_table")

    def test_export_all_data(self):
        mock_carbon_region_data = {"aws:region1": "data1", "aws:region2": "data2"}
        self.carbon_exporter.export_all_data(mock_carbon_region_data)
        self.mock_client.set_value_in_table.assert_called()


if __name__ == "__main__":
    unittest.main()
