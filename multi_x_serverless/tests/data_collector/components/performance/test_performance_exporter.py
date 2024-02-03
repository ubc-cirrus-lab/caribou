import unittest
from unittest.mock import Mock
from multi_x_serverless.data_collector.components.data_exporter import DataExporter
from multi_x_serverless.data_collector.components.performance.performance_exporter import PerformanceExporter

class TestPerformanceExporter(unittest.TestCase):
    def setUp(self):
        self.mock_client = Mock()
        self.performance_exporter = PerformanceExporter(self.mock_client, 'performance_region_table')

    def test_export_all_data(self):
        mock_performance_region_data = {'aws:region1': 'data1', 'aws:region2': 'data2'}
        self.performance_exporter.export_all_data(mock_performance_region_data)
        self.mock_client.set_value_in_table.assert_called()

if __name__ == '__main__':
    unittest.main()