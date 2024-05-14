import unittest
from unittest.mock import MagicMock, Mock, patch
from caribou.data_collector.components.carbon.carbon_retriever import CarbonRetriever


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
                "averages": {"overall": 10.0, **hourly_averages_template.copy()},
                "units": "gCO2eq/kWh",
                "transmission_distances": {},
                "transmission_distances_unit": "km",
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
        self.assertEqual(result, {"aws:region1": 100, "aws:region2": 100})

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

        self.assertEqual(result, {"carbon_intensity": 0})

    def test_get_hour_average_carbon_intensity(self):
        self.carbon_retriever._get_hour_average_carbon_intensity = MagicMock(return_value=1)
        result = self.carbon_retriever._get_hour_average_carbon_intensity(0, 0, 0)
        self.assertEqual(result, 1)

    def test_get_overall_average_carbon_intensity(self):
        self.carbon_retriever._get_carbon_intensity_information = MagicMock(return_value={"overall_average": 2})
        result = self.carbon_retriever._get_overall_average_carbon_intensity(0, 0)
        self.assertEqual(result, 2)

    def test_get_carbon_intensity_information(self):
        self.carbon_retriever._get_raw_carbon_intensity_history_range = MagicMock(return_value=[None])
        self.carbon_retriever._process_raw_carbon_intensity_history = MagicMock(return_value={"test": None})
        result = self.carbon_retriever._get_carbon_intensity_information(0, 0)

        # Check for invocation of the mock objects
        self.carbon_retriever._get_raw_carbon_intensity_history_range.assert_called_once()
        self.carbon_retriever._process_raw_carbon_intensity_history.assert_called_once()

        self.assertEqual(result, {"test": None})

    def test_process_raw_carbon_intensity_history(self):
        result = self.carbon_retriever._process_raw_carbon_intensity_history(
            [
                {"carbonIntensity": 189, "datetime": "2024-05-11T19:00:00.000Z"},
                {"carbonIntensity": 169, "datetime": "2024-05-11T20:00:00.000Z"},
                {"carbonIntensity": 148, "datetime": "2024-05-11T21:00:00.000Z"},
                {"carbonIntensity": 133, "datetime": "2024-05-11T22:00:00.000Z"},
                {"carbonIntensity": 136, "datetime": "2024-05-11T23:00:00.000Z"},
                {"carbonIntensity": 122, "datetime": "2024-05-12T00:00:00.000Z"},
                {"carbonIntensity": 132, "datetime": "2024-05-12T01:00:00.000Z"},
                {"carbonIntensity": 141, "datetime": "2024-05-12T02:00:00.000Z"},
                {"carbonIntensity": 141, "datetime": "2024-05-12T03:00:00.000Z"},
                {"carbonIntensity": 147, "datetime": "2024-05-12T04:00:00.000Z"},
                {"carbonIntensity": 150, "datetime": "2024-05-12T05:00:00.000Z"},
                {"carbonIntensity": 148, "datetime": "2024-05-12T06:00:00.000Z"},
                {"carbonIntensity": 143, "datetime": "2024-05-12T07:00:00.000Z"},
                {"carbonIntensity": 118, "datetime": "2024-05-12T08:00:00.000Z"},
                {"carbonIntensity": 104, "datetime": "2024-05-12T09:00:00.000Z"},
                {"carbonIntensity": 115, "datetime": "2024-05-12T10:00:00.000Z"},
                {"carbonIntensity": 109, "datetime": "2024-05-12T11:00:00.000Z"},
                {"carbonIntensity": 110, "datetime": "2024-05-12T12:00:00.000Z"},
                {"carbonIntensity": 112, "datetime": "2024-05-12T13:00:00.000Z"},
                {"carbonIntensity": 124, "datetime": "2024-05-12T14:00:00.000Z"},
                {"carbonIntensity": 130, "datetime": "2024-05-12T15:00:00.000Z"},
                {"carbonIntensity": 140, "datetime": "2024-05-12T16:00:00.000Z"},
                {"carbonIntensity": 158, "datetime": "2024-05-12T17:00:00.000Z"},
                {"carbonIntensity": 159, "datetime": "2024-05-12T18:00:00.000Z"},
                {"carbonIntensity": 143, "datetime": "2024-05-12T19:00:00.000Z"},
                {"carbonIntensity": 142, "datetime": "2024-05-12T20:00:00.000Z"},
                {"carbonIntensity": 135, "datetime": "2024-05-12T21:00:00.000Z"},
                {"carbonIntensity": 139, "datetime": "2024-05-12T22:00:00.000Z"},
                {"carbonIntensity": 140, "datetime": "2024-05-12T23:00:00.000Z"},
                {"carbonIntensity": 123, "datetime": "2024-05-13T00:00:00.000Z"},
                {"carbonIntensity": 123, "datetime": "2024-05-13T01:00:00.000Z"},
                {"carbonIntensity": 140, "datetime": "2024-05-13T02:00:00.000Z"},
                {"carbonIntensity": 160, "datetime": "2024-05-13T03:00:00.000Z"},
                {"carbonIntensity": 170, "datetime": "2024-05-13T04:00:00.000Z"},
                {"carbonIntensity": 185, "datetime": "2024-05-13T05:00:00.000Z"},
                {"carbonIntensity": 173, "datetime": "2024-05-13T06:00:00.000Z"},
                {"carbonIntensity": 167, "datetime": "2024-05-13T07:00:00.000Z"},
                {"carbonIntensity": 146, "datetime": "2024-05-13T08:00:00.000Z"},
                {"carbonIntensity": 147, "datetime": "2024-05-13T09:00:00.000Z"},
                {"carbonIntensity": 134, "datetime": "2024-05-13T10:00:00.000Z"},
                {"carbonIntensity": 122, "datetime": "2024-05-13T11:00:00.000Z"},
                {"carbonIntensity": 129, "datetime": "2024-05-13T12:00:00.000Z"},
                {"carbonIntensity": 139, "datetime": "2024-05-13T13:00:00.000Z"},
                {"carbonIntensity": 152, "datetime": "2024-05-13T14:00:00.000Z"},
                {"carbonIntensity": 184, "datetime": "2024-05-13T15:00:00.000Z"},
                {"carbonIntensity": 207, "datetime": "2024-05-13T16:00:00.000Z"},
                {"carbonIntensity": 254, "datetime": "2024-05-13T17:00:00.000Z"},
                {"carbonIntensity": 262, "datetime": "2024-05-13T18:00:00.000Z"},
            ]
        )
        expected_result = {
            "overall_average": 212.401977653503,
            "hourly_average": {
                19: 243.72002886206153,
                20: 232.08006401038426,
                21: 216.94018587820892,
                22: 210.30022790723157,
                23: 211.16031423410178,
                0: 194.52028679492105,
                1: 198.38037267588945,
                2: 210.24035638001257,
                3: 219.10036343654593,
                4: 225.96033221719864,
                5: 233.82028751225798,
                6: 225.68031835196723,
                7: 219.0403884445826,
                8: 194.9004558514361,
                9: 187.26060646373222,
                10: 185.12058634443375,
                11: 174.9804751556152,
                12: 177.84050509886416,
                13: 182.7005162156523,
                14: 194.06033894652077,
                15: 211.92025221551722,
                16: 227.28010249965672,
                17: 258.6400982564218,
                18: 261.99999993085714,
            },
        }
        self.assertAlmostEqual(result, expected_result, places=1)

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
