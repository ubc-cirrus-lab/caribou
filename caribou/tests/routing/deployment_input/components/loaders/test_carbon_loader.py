import unittest
from unittest.mock import Mock, patch
from caribou.deployment_solver.deployment_input.components.loaders.carbon_loader import CarbonLoader


class TestCarbonLoader(unittest.TestCase):
    def setUp(self):
        self.client = Mock()
        self.loader = CarbonLoader(self.client)
        self.loader._carbon_data = {
            "aws:eu-south-1": {
                "averages": {
                    "overall": {"carbon_intensity": 482.0},
                    "0": {"carbon_intensity": 482.0},  # 0 is the default hour
                    "1": {"carbon_intensity": 482.0},
                    "2": {"carbon_intensity": 482.0},
                    "3": {"carbon_intensity": 482.0},
                    "4": {"carbon_intensity": 482.0},
                    "5": {"carbon_intensity": 498.0},
                    "6": {"carbon_intensity": 482.0},
                    "7": {"carbon_intensity": 482.0},
                    "8": {"carbon_intensity": 482.0},
                    "9": {"carbon_intensity": 482.0},
                    "10": {"carbon_intensity": 482.0},
                    "11": {"carbon_intensity": 482.0},
                    "12": {"carbon_intensity": 482.0},
                    "13": {"carbon_intensity": 482.0},
                    "14": {"carbon_intensity": 482.0},
                    "15": {"carbon_intensity": 482.0},
                    "16": {"carbon_intensity": 482.0},
                    "17": {"carbon_intensity": 482.0},
                    "18": {"carbon_intensity": 482.0},
                    "19": {"carbon_intensity": 482.0},
                    "20": {"carbon_intensity": 482.0},
                    "21": {"carbon_intensity": 482.0},
                    "22": {"carbon_intensity": 482.0},
                    "23": {"carbon_intensity": 482.0},
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
        self.assertEqual(result, 498.0)


if __name__ == "__main__":
    unittest.main()
