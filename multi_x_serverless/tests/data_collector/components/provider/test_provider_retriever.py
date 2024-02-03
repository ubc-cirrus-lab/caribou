import unittest
from unittest.mock import patch, MagicMock
from multi_x_serverless.data_collector.components.provider.provider_retriever import ProviderRetriever
from multi_x_serverless.common.models.remote_client.remote_client import RemoteClient


class TestProviderRetriever(unittest.TestCase):
    def setUp(self):
        self.remote_client = MagicMock(spec=RemoteClient)
        self.provider_retriever = ProviderRetriever(self.remote_client)

    @patch("googlemaps.Client")
    def test_retrieve_location(self, mock_googlemaps_client):
        # Mock the response from googlemaps.Client
        mock_googlemaps_client.return_value.geocode.return_value = [
            {"geometry": {"location": {"lat": 40.7128, "lng": 74.0060}}}
        ]
        lat, lng = self.provider_retriever.retrieve_location("New York")
        self.assertEqual((lat, lng), (40.7128, 74.0060))

    # Need to add the rest of the functions.


if __name__ == "__main__":
    unittest.main()
