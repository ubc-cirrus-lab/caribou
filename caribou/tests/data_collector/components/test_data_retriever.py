import unittest
from unittest.mock import MagicMock
from caribou.common.models.remote_client.remote_client import RemoteClient
from caribou.data_collector.components.data_retriever import DataRetriever


class TestDataRetriever(unittest.TestCase):
    def setUp(self):
        self.client = MagicMock(spec=RemoteClient)
        self.retriever = DataRetriever(self.client)

    def test_init(self):
        self.assertEqual(self.retriever._client, self.client)
        self.assertEqual(self.retriever._available_regions, {})

    def test_retrieve_available_regions(self):
        mock_regions = {"aws:region1": '{"data": "data1"}', "aws:region2": '{"data": "data2"}'}
        self.client.get_all_values_from_table.return_value = mock_regions

        result = self.retriever.retrieve_available_regions()

        self.client.get_all_values_from_table.assert_called_once_with(self.retriever._available_region_table)
        self.assertEqual(result, {"aws:region1": {"data": "data1"}, "aws:region2": {"data": "data2"}})
        self.assertEqual(
            self.retriever._available_regions, {"aws:region1": {"data": "data1"}, "aws:region2": {"data": "data2"}}
        )


if __name__ == "__main__":
    unittest.main()
