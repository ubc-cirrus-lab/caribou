import unittest
from unittest.mock import patch, Mock
from multi_x_serverless.data_collector.utils.latency_retriever.aws_latency_retriever import AWSLatencyRetriever


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

    @patch("requests.get")
    def test_parse_table(self, mock_get):
        # Create a mock response with a simple HTML table
        mock_response = Mock()
        mock_response.content = self.html_table
        mock_get.return_value = mock_response

        # Create an AWSLatencyRetriever instance and parse the table
        retriever = AWSLatencyRetriever()
        retriever._parse_table()

        # Check if the table was parsed correctly
        self.assertEqual(retriever.columns, ["af-south-1", "ap-east-1", "af-south-1", "ap-east-1"])
        self.assertEqual(retriever.data, {"af-south-1": ["11.56", "247.76"], "ap-east-1": ["256.59", "3.89"]})

    @patch("requests.get")
    def test_get_latency(self, mock_get):
        # Create a mock response with a simple HTML table
        mock_response = Mock()
        mock_response.content = self.html_table
        mock_get.return_value = mock_response

        # Create an AWSLatencyRetriever instance and parse the table
        retriever = AWSLatencyRetriever()
        retriever._parse_table()

        # Check if the get_latency method returns the correct latency
        self.assertEqual(retriever.get_latency({"code": "af-south-1"}, {"code": "af-south-1"}), 11.56)
        self.assertEqual(retriever.get_latency({"code": "af-south-1"}, {"code": "ap-east-1"}), 247.76)
        self.assertEqual(retriever.get_latency({"code": "ap-east-1"}, {"code": "af-south-1"}), 256.59)
        self.assertEqual(retriever.get_latency({"code": "ap-east-1"}, {"code": "ap-east-1"}), 3.89)


if __name__ == "__main__":
    unittest.main()
