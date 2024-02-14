import unittest
from unittest.mock import Mock, patch
from multi_x_serverless.common.models.remote_client.remote_client import RemoteClient
from multi_x_serverless.data_collector.components.carbon.carbon_transmission_cost_calculator.carbon_transmission_cost_calculator import (
    CarbonTransmissionCostCalculator,
)
from multi_x_serverless.data_collector.components.carbon.carbon_retriever import CarbonRetriever


class TestCarbonRetriever(unittest.TestCase):
    def setUp(self):
        self.mock_client = Mock()
        with patch("os.environ.get") as mock_os_environ_get:
            mock_os_environ_get.return_value = "mock_token"
            self.carbon_retriever = CarbonRetriever(self.mock_client)
        self.maxDiff = None

    @patch("requests.get")
    def test_retrieve_carbon_region_data(self, mock_get):
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = {"carbonIntensity": 100}

        self.carbon_retriever._available_regions = {
            "aws:region1": {"latitude": 1.0, "longitude": 1.0},
            "aws:region2": {"latitude": 2.0, "longitude": 2.0},
        }

        result = self.carbon_retriever.retrieve_carbon_region_data()

        expected_result = {
            "aws:region1": {
                "carbon_intensity": 100,
                "unit": "gCO2eq/kWh",
                "transmission_carbon": {
                    "aws:region1": {"carbon_intensity": 0, "distance": 0.0, "unit": "gCO2eq/GB"},
                    "aws:region2": {"carbon_intensity": 100.0, "distance": 157.22543203807288, "unit": "gCO2eq/GB"},
                },
            },
            "aws:region2": {
                "carbon_intensity": 100,
                "unit": "gCO2eq/kWh",
                "transmission_carbon": {
                    "aws:region1": {"carbon_intensity": 100.0, "distance": 157.22543203807288, "unit": "gCO2eq/GB"},
                    "aws:region2": {"carbon_intensity": 0, "distance": 0.0, "unit": "gCO2eq/GB"},
                },
            },
        }

        self.assertEqual(result, expected_result)

    @patch("requests.get")
    def test_get_carbon_intensity_from_coordinates_success(self, mock_get):
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = {"carbonIntensity": 100}

        result = self.carbon_retriever._get_carbon_intensity_from_coordinates(1.0, 1.0)

        self.assertEqual(result, 100)

    @patch("requests.get")
    def test_get_carbon_intensity_from_coordinates_no_data(self, mock_get):
        mock_get.return_value.status_code = 404
        mock_get.return_value.text = "No recent data for zone"

        result = self.carbon_retriever._get_carbon_intensity_from_coordinates(1.0, 1.0)

        self.assertEqual(result, self.carbon_retriever._global_average_worst_case_carbon_intensity)

    @patch("requests.get")
    def test_get_carbon_intensity_from_coordinates_failure(self, mock_get):
        mock_get.return_value.status_code = 500
        mock_get.return_value.text = "Server error"

        self.assertEqual(
            self.carbon_retriever._get_carbon_intensity_from_coordinates(1.0, 1.0),
            self.carbon_retriever._global_average_worst_case_carbon_intensity,
        )


if __name__ == "__main__":
    unittest.main()
