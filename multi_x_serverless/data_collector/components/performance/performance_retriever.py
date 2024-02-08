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
        self._aws_average_latency_retriever = AWSLatencyRetriever(False)
        self._aws_tail_latency_retriever = AWSLatencyRetriever(True)
        self._integration_test_average_latency_retriever = IntegrationTestLatencyRetriever(False)
        self._integration_test_tail_latency_retriever = IntegrationTestLatencyRetriever(True)
        self._modified_regions: set[str] = set()

    def retrieve_runtime_region_data(self) -> dict[str, dict[str, Any]]:
        result_dict: dict[str, dict[str, Any]] = {}
        for region_key, available_region in self._available_regions.items():
            transmission_latency_dict = {}

            for region_key_to, available_region_to in self._available_regions.items():
                transmission_latency_dict[region_key_to] = {
                    "average_latency": self._get_total_latency(available_region, available_region_to, False),
                    "tail_latency": self._get_total_latency(available_region, available_region_to, True),
                    "unit": "s",
                }

            result_dict[region_key] = {
                "relative_performance": 1,
                # Current assumption is that all regions have the same performance
                # (At least within the same provider)
                "transmission_latency": transmission_latency_dict,
            }
        return result_dict

    def _get_total_latency(
        self, region_from: dict[str, Any], region_to: dict[str, Any], utilize_tail_latency: bool = False
    ) -> float:
        if region_from["provider"] == region_to["provider"]:
            if region_from["provider"] == Provider.AWS.value:
                if utilize_tail_latency:
                    return (
                        self._aws_tail_latency_retriever.get_latency(region_from, region_to) / 1000
                    )  # Convert to seconds
                return (
                    self._aws_average_latency_retriever.get_latency(region_from, region_to) / 1000
                )  # Convert to seconds
            if region_from["provider"] == Provider.INTEGRATION_TEST_PROVIDER.value:
                if utilize_tail_latency:
                    return self._integration_test_tail_latency_retriever.get_latency(region_from, region_to) / 1000

                return (
                    self._integration_test_average_latency_retriever.get_latency(region_from, region_to) / 1000
                )  # Convert to seconds

        return 0.0  # Default value, maybe a better default or an error message will be desired
