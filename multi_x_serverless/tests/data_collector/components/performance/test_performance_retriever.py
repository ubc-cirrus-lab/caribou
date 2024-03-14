import unittest
from unittest.mock import MagicMock, patch
from multi_x_serverless.data_collector.components.performance.performance_retriever import PerformanceRetriever
from multi_x_serverless.common.provider import Provider


class TestPerformanceRetriever(unittest.TestCase):
    @patch("multi_x_serverless.data_collector.components.performance.performance_retriever.AWSLatencyRetriever")
    @patch(
        "multi_x_serverless.data_collector.components.performance.performance_retriever.IntegrationTestLatencyRetriever"
    )
    def test_retrieve_runtime_region_data(self, mock_integration_test_latency_retriever, mock_aws_latency_retriever):
        # Arrange
        mock_client = MagicMock()
        performance_retriever = PerformanceRetriever(mock_client)
        performance_retriever._available_regions = {
            "region1": {"provider": Provider.AWS.value, "region": "us-west-1"},
            "region2": {"provider": Provider.AWS.value, "region": "us-west-2"},
        }
        mock_aws_latency_retriever().get_latency_distribution.side_effect = [
            [0.1, 0.2, 0.3],
            [0.4, 0.5, 0.6],
            [0.1, 0.2, 0.3],
            [0.4, 0.5, 0.6],
        ]

        # Act
        result = performance_retriever.retrieve_runtime_region_data()

        # Assert
        self.assertEqual(result["region1"]["relative_performance"], 1)
        self.assertEqual(result["region1"]["transmission_latency"]["region1"]["latency_distribution"], [0.1, 0.2, 0.3])
        self.assertEqual(result["region1"]["transmission_latency"]["region1"]["unit"], "s")
        self.assertEqual(result["region2"]["relative_performance"], 1)
        self.assertEqual(result["region2"]["transmission_latency"]["region2"]["latency_distribution"], [0.4, 0.5, 0.6])
        self.assertEqual(result["region2"]["transmission_latency"]["region2"]["unit"], "s")


if __name__ == "__main__":
    unittest.main()
