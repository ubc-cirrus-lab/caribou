import unittest
from unittest.mock import Mock, patch
from multi_x_serverless.common.models.remote_client.remote_client import RemoteClient
from multi_x_serverless.data_collector.utils.aws_latency_retriever import AWSLatencyRetriever
from multi_x_serverless.data_collector.components.performance.performance_retriever import PerformanceRetriever

class TestPerformanceRetriever(unittest.TestCase):
    def setUp(self):
        self.client = Mock(spec=RemoteClient)
        self.retriever = PerformanceRetriever(self.client)

    @patch.object(AWSLatencyRetriever, 'get_latency')
    def test_retrieve_runtime_region_data(self, mock_get_latency):
        def custom_get_latency(from_region, to_region):
            # Specify different return values based on the input region
            if from_region == "aws:region1":
                return 5.0
            elif from_region == "aws:region2":
                return 15.0
            else:
                return 0.0

        mock_get_latency.side_effect = custom_get_latency

        self.retriever._available_regions = {
            "aws:region1": {"provider": "aws", "code": "region1"},
            "aws:region1": {"provider": "aws", "code": "region2"}
        }
        expected_result = {
            "aws:region1": {
                "relative_performance": 1,
                "transmission_latency": {
                    "aws:region1": {"transmission_latency": 5.0, "unit": "ms"},
                    "aws:region2": {"transmission_latency": 5.0, "unit": "ms"}
                }
            },
            "aws:region2": {
                "relative_performance": 1,
                "transmission_latency": {
                    "aws:region1": {"transmission_latency": 15.0, "unit": "ms"},
                    "aws:region2": {"transmission_latency": 15.0, "unit": "ms"}
                }
            }
        }
        result = self.retriever.retrieve_runtime_region_data()
        # self.assertEqual(result, expected_result)
        print(result)

    def test_get_total_latency(self):
        region_from = {"provider": "aws"}
        region_to = {"provider": "aws"}
        with patch.object(self.retriever._aws_latency_retriever, 'get_latency', return_value=10.0) as mock_get_latency:
            result = self.retriever._get_total_latency(region_from, region_to)
            mock_get_latency.assert_called_once_with(region_from, region_to)
            self.assertEqual(result, 10.0)

        region_to = {"provider": "Other"}
        result = self.retriever._get_total_latency(region_from, region_to)
        self.assertEqual(result, 0.0)

if __name__ == '__main__':
    unittest.main()