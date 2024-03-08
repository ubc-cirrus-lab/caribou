import unittest
from unittest.mock import Mock, patch
from multi_x_serverless.routing.deployment_input.components.loaders.carbon_loader import CarbonLoader


class TestCarbonLoader(unittest.TestCase):
    def setUp(self):
        self.client = Mock()
        self.loader = CarbonLoader(self.client)
        self.loader._carbon_data = {
            "aws:eu-south-1": {
                "averages": {
                    "overall": 482.0,
                    "0": 10.0, "1": 10.0, "2": 10.0, "3": 10.0, "4": 10.0, "5": 15.0, "6": 10.0,
                    "7": 10.0, "8": 10.0, "9": 10.0, "10": 10.0, "11": 10.0, "12": 10.0, "13": 10.0,
                    "14": 10.0, "15": 10.0, "16": 10.0, "17": 10.0, "18": 10.0, "19": 10.0, "20": 10.0,
                    "21": 10.0, "22": 10.0, "23": 10.0
                },
                "units": "gCO2eq/kWh",
                "transmission_distances": {"aws:eu-south-1": 0, "aws:eu-south-2": 111.19},
                "transmission_distances_unit": "km",
            }
        }

    def test_init(self):
        self.assertEqual(self.loader._client, self.client)

    @patch.object(CarbonLoader, "_retrieve_region_data")
    def test_setup(self, mock_retrieve_region_data):
        mock_retrieve_region_data.return_value = self.loader._carbon_data
        self.loader.setup({"aws:eu-south-1"})
        self.assertEqual(self.loader._carbon_data, self.loader._carbon_data)

    def test_get_transmission_distance(self):
        result = self.loader.get_transmission_distance("aws:eu-south-1", "aws:eu-south-2")
        self.assertEqual(result, 111.19)

    def test_get_grid_carbon_intensity(self):
        result = self.loader.get_grid_carbon_intensity("aws:eu-south-1")
        self.assertEqual(result, 482.0)

        result = self.loader.get_grid_carbon_intensity("aws:eu-south-1", str(5))
        self.assertEqual(result, 15.0)

if __name__ == "__main__":
    unittest.main()
