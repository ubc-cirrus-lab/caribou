import unittest
from unittest.mock import MagicMock
from multi_x_serverless.routing.deployment_input.components.loaders.region_viability_loader import RegionViabilityLoader


class TestRegionViabilityLoader(unittest.TestCase):
    def setUp(self):
        self.mock_client = MagicMock()
        self.loader = RegionViabilityLoader(self.mock_client)

    def test_setup(self):
        # Mocking the response from the client
        mocked_response = {
            "aws:eu-south-1": {
                "key": "aws:eu-south-1",
                "provider_collector": 1620000000,
                "carbon_collector": 1620000000,
                "performance_collector": 1620000000,
                "value": {
                    "name": "Europe (Milan)",
                    "provider": "aws",
                    "code": "eu-south-1",
                    "latitude": 45.4642035,
                    "longitude": 9.189982,
                },
            }
        }
        self.mock_client.get_all_values_from_table.return_value = mocked_response

        # Call the setup method
        self.loader.setup()

        # Assert that available regions are populated correctly
        self.assertEqual(len(self.loader.get_available_regions()), 1)
        self.assertIn("aws:eu-south-1", self.loader.get_available_regions())

    def test_get_available_regions(self):
        # Mocking the available regions
        self.loader._available_regions = ["aws:eu-south-1", "gcp:us-west1"]

        # Call the get_available_regions method
        available_regions = self.loader.get_available_regions()

        # Assert that the returned available regions match the mocked ones
        self.assertEqual(available_regions, ["aws:eu-south-1", "gcp:us-west1"])


if __name__ == "__main__":
    unittest.main()
