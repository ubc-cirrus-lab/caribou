import unittest
from unittest.mock import patch, Mock
from multi_x_serverless.data_collector.utils.latency_retriever.aws_latency_retriever import AWSLatencyRetriever
from bs4 import BeautifulSoup


class TestAWSLatencyRetriever(unittest.TestCase):
    def setUp(self):
        self.html_table = b"""
        <table class="table table-bordered table-sm">
            <thead class="thead-light">
                <tr>
                <th style="padding: 0px;" class="th_msg">
                    <table align="left" style="width: 100%" class="loc_table">
                    <tbody>
                        <tr>
                        <td class="destination">Destination Region</td>
                        </tr>
                        <tr>
                        <td class="source" style="background-color: #fff;">Source Region</td>
                        </tr>
                    </tbody>
                    </table>
                </th>
                <th scope="col" class="region_title">Africa (Cape Town)<br> <em>af-south-1</em></th>
                <th scope="col" class="region_title">Asia Pacific (Hong Kong)<br> <em>ap-east-1</em></th>
                </tr>
            </thead>
            <tbody>
                <tr>
                <th scope="row" class="region_title">Africa (Cape Town)<br> <em>af-south-1</em></th>
                <td class="green">11.56</td>
                <td class="red">247.76</td>
                </tr>
                <tr>
                <th scope="row" class="region_title">Asia Pacific (Hong Kong)<br> <em>ap-east-1</em></th>
                <td class="red">256.59</td>
                <td class="green">3.89</td>
                </tr>
            </tbody>
        </table>
        """

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
        mock_response = Mock()
        mock_response.content = self.html_table
        mock_get.return_value = mock_response
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

    def test_parse_table(self):
        # Arrange
        aws_latency_retriever = AWSLatencyRetriever()
        soup = BeautifulSoup(self.html_table, "html.parser")

        # Act
        parsed_table = aws_latency_retriever._parse_table(soup)

        # Assert
        # Replace this with your actual expected output
        expected_output = {
            "af-south-1": {"af-south-1": 11.56, "ap-east-1": 247.76},
            "ap-east-1": {"af-south-1": 256.59, "ap-east-1": 3.89},
        }
        self.assertEqual(parsed_table, expected_output)

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
