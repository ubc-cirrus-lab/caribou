import unittest
from unittest.mock import patch, Mock
from caribou.data_collector.utils.latency_retriever.aws_latency_retriever import AWSLatencyRetriever


class TestAWSLatencyRetriever(unittest.TestCase):
    def setUp(self):
        mock_latency_data = {
            "af-south-1": {"af-south-1": 11.56, "ap-east-1": 247.76},
            "ap-east-1": {"af-south-1": 256.59, "ap-east-1": 3.89},
        }

        percentiles = ["p_10", "p_25", "p_50", "p_75", "p_90", "p_98", "p_99"]
        self.mock_responses = {}
        for p in percentiles:
            mock_json = {
                "metadata": {"percentile": p, "timeframe": "1W", "unit": "milliseconds"},
                "data": mock_latency_data,
            }
            mock_resp = Mock(status_code=200)
            mock_resp.json.return_value = mock_json
            mock_resp.raise_for_status.return_value = None
            self.mock_responses[p] = mock_resp

        self.mock_response_sequence = [self.mock_responses[p] for p in percentiles]

        self.percentile_information = {
            "us-west-2": {
                "us-west-2": {
                    "p_10": 11.56,
                    "p_25": 11.56,
                    "p_50": 11.56,
                    "p_75": 11.56,
                    "p_90": 11.56,
                    "p_98": 11.56,
                    "p_99": 11.56,
                },
                "me-south-1": {
                    "p_10": 247.76,
                    "p_25": 247.76,
                    "p_50": 247.76,
                    "p_75": 247.76,
                    "p_90": 247.76,
                    "p_98": 247.76,
                    "p_99": 247.76,
                },
            },
            "me-south-1": {
                "us-west-2": {
                    "p_10": 256.59,
                    "p_25": 256.59,
                    "p_50": 256.59,
                    "p_75": 256.59,
                    "p_90": 256.59,
                    "p_98": 256.59,
                    "p_99": 256.59,
                },
                "me-south-1": {
                    "p_10": 3.89,
                    "p_25": 3.89,
                    "p_50": 3.89,
                    "p_75": 3.89,
                    "p_90": 3.89,
                    "p_98": 3.89,
                    "p_99": 3.89,
                },
            },
        }

    @patch("requests.get")
    def test_get_percentile_information(self, mock_get):
        # Arrange
        mock_get.side_effect = self.mock_response_sequence
        aws_latency_retriever = AWSLatencyRetriever()

        # Act
        percentile_information = aws_latency_retriever._get_percentile_information()

        # Assert
        # Replace this with your actual expected output
        expected_output = {
            "af-south-1": {
                "af-south-1": {
                    "p_10": 11.56,
                    "p_25": 11.56,
                    "p_50": 11.56,
                    "p_75": 11.56,
                    "p_90": 11.56,
                    "p_98": 11.56,
                    "p_99": 11.56,
                },
                "ap-east-1": {
                    "p_10": 247.76,
                    "p_25": 247.76,
                    "p_50": 247.76,
                    "p_75": 247.76,
                    "p_90": 247.76,
                    "p_98": 247.76,
                    "p_99": 247.76,
                },
            },
            "ap-east-1": {
                "af-south-1": {
                    "p_10": 256.59,
                    "p_25": 256.59,
                    "p_50": 256.59,
                    "p_75": 256.59,
                    "p_90": 256.59,
                    "p_98": 256.59,
                    "p_99": 256.59,
                },
                "ap-east-1": {
                    "p_10": 3.89,
                    "p_25": 3.89,
                    "p_50": 3.89,
                    "p_75": 3.89,
                    "p_90": 3.89,
                    "p_98": 3.89,
                    "p_99": 3.89,
                },
            },
        }
        self.assertEqual(percentile_information, expected_output)

    def test_get_latency_distribution(self):
        # Arrange
        aws_latency_retriever = AWSLatencyRetriever()
        aws_latency_retriever._percentile_information = self.percentile_information

        # Act
        latency_distribution = aws_latency_retriever.get_latency_distribution(
            {"code": "us-west-2"}, {"code": "me-south-1"}
        )

        # Assert
        # As the output is random, we can only check the length of the output and the range of the values
        self.assertEqual(len(latency_distribution), 100)
        self.assertTrue(all(0 <= x <= 0.3 for x in latency_distribution))


if __name__ == "__main__":
    unittest.main()
