import time
import unittest
from unittest.mock import MagicMock, patch
from multi_x_serverless.common.models.remote_client.remote_client import RemoteClient
from multi_x_serverless.data_collector.components.data_exporter import DataExporter


class TestDataExporter(unittest.TestCase):
    def setUp(self):
        self.client = MagicMock(spec=RemoteClient)
        self.exporter = DataExporter(self.client, "region_table")

    def test_init(self):
        self.assertEqual(self.exporter._client, self.client)
        self.assertEqual(self.exporter._region_table, "region_table")
        self.assertEqual(self.exporter._modified_regions, set())

    def test_update_available_region_timestamp(self):
        with patch("time.time", return_value=1706909825.0010574):
            self.exporter.update_available_region_timestamp("data_collector_name", {"aws:region1", "aws:region2"})
        self.client.set_value_in_table_column.assert_any_call(
            self.exporter._available_region_table,
            "aws:region1",
            column_type_value=[("data_collector_name", "N", "1706909825.0010574")],
        )
        self.client.set_value_in_table_column.assert_any_call(
            self.exporter._available_region_table,
            "aws:region2",
            column_type_value=[("data_collector_name", "N", "1706909825.0010574")],
        )

    def test_get_modified_regions(self):
        self.exporter._modified_regions = {"aws:region1", "aws:region2"}
        self.assertEqual(self.exporter.get_modified_regions(), {"aws:region1", "aws:region2"})

    def test_export_region_data(self):
        region_data = {"aws:region1": {}, "aws:region2": {}}
        self.exporter._export_data = MagicMock()
        self.exporter._export_region_data(region_data)
        self.exporter._export_data.assert_called_once_with("region_table", region_data, True)

    def test_update_modified_regions(self):
        self.exporter._update_modified_regions("aws", "region1")
        self.assertEqual(self.exporter._modified_regions, {"aws:region1"})

    def test_export_data(self):
        data = {
            "aws:region1": {"data": "data1"},
            "aws:region2": {"data": "data2"},
        }
        self.exporter._export_data("table_name", data, True)

        # Check that set_value_in_table was called with specific arguments
        self.client.set_value_in_table.assert_any_call(
            "table_name", "aws:region1", '{"data": "data1"}'
        )
        self.client.set_value_in_table.assert_any_call(
            "table_name", "aws:region2", '{"data": "data2"}'
        )

        # Check the total number of calls
        self.assertEqual(self.client.set_value_in_table.call_count, 2)

        self.assertIn("aws:region1", self.exporter._modified_regions)
        self.assertIn("aws:region2", self.exporter._modified_regions)


if __name__ == "__main__":
    unittest.main()
