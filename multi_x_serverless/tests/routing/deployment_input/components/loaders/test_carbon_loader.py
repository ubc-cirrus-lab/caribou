import unittest
from unittest.mock import Mock, patch
from multi_x_serverless.routing.deployment_input.components.loaders.carbon_loader import CarbonLoader


class TestCarbonLoader(unittest.TestCase):
    def setUp(self):
        self.client = Mock()
        self.loader = CarbonLoader(self.client)
        self.loader._carbon_data = {
            "aws:eu-south-1": {
                "overall_average": {
                    "carbon_intensity": 482,
                    "unit": "gCO2eq/kWh",
                    "transmission_carbon": {
                        "aws:eu-south-1": {"carbon_intensity": 48.2, "unit": "gCO2eq/GB"},
                        "aws:eu-central-1": {"carbon_intensity": 1337.9261964617801, "unit": "gCO2eq/GB"},
                        "aws:us-west-2": {"carbon_intensity": 21269.19652594863, "unit": "gCO2eq/GB"},
                    },
                },
                "hourly_averages": {
                    "1": {
                        "carbon_intensity": 482,
                        "unit": "gCO2eq/kWh",
                        "transmission_carbon": {
                            "aws:eu-south-1": {"carbon_intensity": 48.2, "unit": "gCO2eq/GB"},
                            "aws:eu-central-1": {"carbon_intensity": 1337.9261964617801, "unit": "gCO2eq/GB"},
                            "aws:us-west-2": {"carbon_intensity": 21269.19652594863, "unit": "gCO2eq/GB"},
                        },
                    },
                },
            }
        }

    def test_init(self):
        self.assertEqual(self.loader._client, self.client)

    @patch.object(CarbonLoader, "_retrieve_region_data")
    def test_setup(self, mock_retrieve_region_data):
        mock_retrieve_region_data.return_value = self.loader._carbon_data
        self.loader.setup({"aws:eu-south-1"})
        self.assertEqual(self.loader._carbon_data, self.loader._carbon_data)

    def test_get_transmission_carbon_intensity(self):
        result = self.loader.get_transmission_carbon_intensity("aws:eu-south-1", "aws:eu-central-1")
        self.assertEqual(result, (1337.9261964617801, 1000.0))

    def test_get_grid_carbon_intensity(self):
        result = self.loader.get_grid_carbon_intensity("aws:eu-south-1")
        self.assertEqual(result, 482)


if __name__ == "__main__":
    unittest.main()
