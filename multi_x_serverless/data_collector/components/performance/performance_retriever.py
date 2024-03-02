from typing import Any

from multi_x_serverless.common.models.remote_client.remote_client import RemoteClient
from multi_x_serverless.common.provider import Provider
from multi_x_serverless.data_collector.components.data_retriever import DataRetriever
from multi_x_serverless.data_collector.utils.latency_retriever.aws_latency_retriever import AWSLatencyRetriever
from multi_x_serverless.data_collector.utils.latency_retriever.integration_test_latency_retriever import (
    IntegrationTestLatencyRetriever,
)


class PerformanceRetriever(DataRetriever):
    def __init__(self, client: RemoteClient) -> None:
        super().__init__(client)
        self._aws_latency_retriever = AWSLatencyRetriever()
        self._integration_test_latency_retriever = IntegrationTestLatencyRetriever()
        self._modified_regions: set[str] = set()

    def retrieve_runtime_region_data(self) -> dict[str, dict[str, Any]]:
        result_dict: dict[str, dict[str, Any]] = {}
        for region_key, available_region in self._available_regions.items():
            transmission_latency_dict = {}

            for region_key_to, available_region_to in self._available_regions.items():
                latency_distribution = self._get_latency_distribution(available_region, available_region_to)

                transmission_latency_dict[region_key_to] = {
                    "latency_distribution": [latency_distribution],
                    "unit": "s",
                }

            # Current assumption is that all regions have the same performance
            # (At least within the same provider)
            result_dict[region_key] = {
                "relative_performance": 1,
                "transmission_latency": transmission_latency_dict,
            }
        return result_dict

    def _get_latency_distribution(self, region_from: dict[str, Any], region_to: dict[str, Any]) -> list[float]:
        try:
            if region_from["provider"] == region_to["provider"]:
                if region_from["provider"] == Provider.AWS.value:
                    return self._aws_latency_retriever.get_latency_distribution(region_from, region_to)
                if region_from["provider"] == Provider.INTEGRATION_TEST_PROVIDER.value:
                    return self._integration_test_latency_retriever.get_latency_distribution(region_from, region_to)
        except ValueError:
            return []

        return []
