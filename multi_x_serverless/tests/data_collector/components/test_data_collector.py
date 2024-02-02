import unittest
from unittest.mock import patch, MagicMock
from multi_x_serverless.common.models.endpoints import Endpoints
from multi_x_serverless.data_collector.components.data_collector import DataCollector

class MockDataCollector(DataCollector):
    def __init__(self):
        super().__init__()

    def run(self):
        pass

class TestDataCollector(unittest.TestCase):
    @patch.object(Endpoints, 'get_data_collector_client')
    def test_init(self, mock_get_data_collector_client):
        mock_client = MagicMock()
        mock_get_data_collector_client.return_value = mock_client

        collector = MockDataCollector()

        self.assertEqual(collector._data_collector_client, mock_client)
        self.assertEqual(collector._available_region_data, {})

    def test_run(self):
        collector = MockDataCollector()
        # Since run method doesn't do anything in our mock class, we just test if it runs without errors
        try:
            collector.run()
        except Exception as e:
            self.fail(f"run method raised an exception: {e}")

if __name__ == '__main__':
    unittest.main()