import unittest
from unittest.mock import MagicMock, Mock, patch
from multi_x_serverless.data_collector.components.carbon.carbon_retriever import CarbonRetriever


class TestCarbonRetriever(unittest.TestCase):
    def setUp(self):
        self.mock_client = Mock()
        with patch("os.environ.get") as mock_os_environ_get:
            mock_os_environ_get.return_value = "mock_token"
            self.carbon_retriever = CarbonRetriever(self.mock_client)
        self.maxDiff = None

    def test_init(self):
        self.assertIsInstance(self.carbon_retriever, CarbonRetriever)

    @patch.object(CarbonRetriever, "_get_distance_between_all_regions")
    @patch.object(CarbonRetriever, "_get_execution_carbon_intensity")
    def test_retrieve_carbon_region_data(self, mock_get_carbon_intensity, mock_get_distance):
        self.carbon_retriever._available_regions = {
            "aws:region1": {"latitude": 1.0, "longitude": 1.0},
            "aws:region2": {"latitude": 2.0, "longitude": 2.0},
        }
        mock_get_carbon_intensity.return_value = 10.0
        mock_get_distance.return_value = {}
        result = self.carbon_retriever.retrieve_carbon_region_data()

        hourly_averages_template = {str(hour): 10.0 for hour in range(24)}
        expected_result = {
            region_id: {
                "averages": {
                    "overall": 10.0,
                    **hourly_averages_template.copy()
                },
                'units': 'gCO2eq/kWh',
                'transmission_distances': {},
                'transmission_distances_unit': 'km'
            }
            for region_id in self.carbon_retriever._available_regions.keys()
        }
        self.assertEqual(result, expected_result)
        
    def test_get_distance_between_all_regions(self):
        all_regions = {
            "aws:region1": {"latitude": 1.0, "longitude": 1.0},
            "aws:region2": {"latitude": 2.0, "longitude": 2.0},
        }
        self.carbon_retriever._available_regions = all_regions
        self.carbon_retriever._get_distance_between_coordinates = MagicMock(return_value=100)
        result = self.carbon_retriever._get_distance_between_all_regions(all_regions["aws:region1"])
        self.assertEqual(result, {'aws:region1': 100, 'aws:region2': 100})

    def test_get_distance_between_coordinates(self):
        result = self.carbon_retriever._get_distance_between_coordinates(1.0, 1.0, 1.0, 1.0)
        self.assertEqual(result, 0)

        result = self.carbon_retriever._get_distance_between_coordinates(0, 0, 0, 1)
        self.assertAlmostEqual(result, 111.19, places=2)

    def test_get_execution_carbon_intensity(self):
        # Setup the mock object and its return value
        # Call the method under test
        result = self.carbon_retriever._get_execution_carbon_intensity(
            {"latitude": 1.0, "longitude": 1.0}, MagicMock(return_value=0)
        )

        self.assertEqual(
            result,
            {'carbon_intensity': 0}
        )

    def test_get_hour_average_carbon_intensity(self):
        self.carbon_retriever._get_hour_average_carbon_intensity = MagicMock(return_value=1)
        result = self.carbon_retriever._get_hour_average_carbon_intensity(0, 0, 0)
        self.assertEqual(result, 1)

    def test_get_overall_average_carbon_intensity(self):
        self.carbon_retriever._get_carbon_intensity_information = MagicMock(return_value={"overall_average": 2})
        result = self.carbon_retriever._get_overall_average_carbon_intensity(0, 0)
        self.assertEqual(result, 2)

    def test_get_carbon_intensity_information(self):
        self.carbon_retriever._get_raw_carbon_intensity_history_range = MagicMock(return_value={})
        self.carbon_retriever._process_raw_carbon_intensity_history = MagicMock(return_value={"test": None})
        result = self.carbon_retriever._get_carbon_intensity_information(0, 0)

        # Check for invocation of the mock objects
        self.carbon_retriever._get_raw_carbon_intensity_history_range.assert_called_once()
        self.carbon_retriever._process_raw_carbon_intensity_history.assert_called_once()

        self.assertEqual(result, {"test": None})

    def test_process_raw_carbon_intensity_history(self):
        result = self.carbon_retriever._process_raw_carbon_intensity_history([])
        expected_result = {
            "overall_average": 475.0,
            "hourly_average": {hour: 475.0 for hour in range(24)},
        }
        self.assertEqual(result, expected_result)

        result = self.carbon_retriever._process_raw_carbon_intensity_history(
            [
                {"carbonIntensity": 100, "datetime": "2021-01-01T00:00:00Z"},
                {"carbonIntensity": 200, "datetime": "2021-02-01T00:00:00Z"},
                {"carbonIntensity": 175, "datetime": "2021-02-01T01:00:00Z"},
            ]
        )
        expected_result = {"overall_average": 158.33333333333334, "hourly_average": {0: 150.0, 1: 175.0}}
        self.assertEqual(result, expected_result)

    @patch("requests.get")
    def test_get_raw_carbon_intensity_history_range(self, mock_get):
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = {
            "data": [{"carbonIntensity": 100, "datetime": "2021-01-01T00:00:00Z"}]
        }

        result = self.carbon_retriever._get_raw_carbon_intensity_history_range(1.0, 1.0, "", "")
        self.assertEqual(result, [{"carbonIntensity": 100, "datetime": "2021-01-01T00:00:00Z"}])

    @patch("requests.get")
    def test_get_raw_carbon_intensity_history_range_no_data(self, mock_get):
        mock_get.return_value.status_code = 404
        mock_get.return_value.text = "No recent data for zone"

        result = self.carbon_retriever._get_raw_carbon_intensity_history_range(1.0, 1.0, "", "")

        self.assertEqual(result, [])

    @patch("requests.get")
    def test_get_raw_carbon_intensity_history_range_failure(self, mock_get):
        mock_get.return_value.status_code = 500
        mock_get.return_value.text = "Server error"

        result = self.carbon_retriever._get_raw_carbon_intensity_history_range(1.0, 1.0, "", "")

        self.assertEqual(result, [])

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
