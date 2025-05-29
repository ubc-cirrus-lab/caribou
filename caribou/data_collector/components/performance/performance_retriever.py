from typing import Any

from caribou.common.models.remote_client.remote_client import RemoteClient
from caribou.common.provider import Provider
from caribou.data_collector.components.data_retriever import DataRetriever
from caribou.data_collector.utils.latency_retriever.aws_latency_retriever import AWSLatencyRetriever
from caribou.data_collector.utils.latency_retriever.gcp_latency_retriever import GCPLatencyRetriever
from caribou.data_collector.utils.latency_retriever.integration_test_latency_retriever import (
    IntegrationTestLatencyRetriever,
)


class PerformanceRetriever(DataRetriever):
    def __init__(self, client: RemoteClient) -> None:
        super().__init__(client)
        self._aws_latency_retriever = AWSLatencyRetriever()
        self._gcp_latency_retriever = GCPLatencyRetriever()
        self._integration_test_latency_retriever = IntegrationTestLatencyRetriever()
        self._modified_regions: set[str] = set()
        self._latency_distribution_cache: dict[str, list[float]] = {}

    def retrieve_runtime_region_data(self) -> dict[str, dict[str, Any]]:
        result_dict: dict[str, dict[str, Any]] = {}
        for region_key, available_region in self._available_regions.items():
            transmission_latency_dict = {}

            for region_key_to, available_region_to in self._available_regions.items():
                latency_distribution = self._get_latency_distribution(available_region, available_region_to)

                transmission_latency_dict[region_key_to] = {
                    "latency_distribution": latency_distribution,
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
        cache_key = f"{region_from['provider']}_{region_from['code']}_{region_to['provider']}_{region_to['code']}"
        if cache_key in self._latency_distribution_cache:
            return self._latency_distribution_cache[cache_key]
        try:
            latency_distribution = []
            if region_from["provider"] == region_to["provider"]:
                if region_from["provider"] == Provider.AWS.value:
                    latency_distribution = self._aws_latency_retriever.get_latency_distribution(region_from, region_to)
                elif region_from["provider"] == Provider.GCP.value:
                    latency_distribution = self._gcp_latency_retriever.get_latency_distribution(region_from, region_to)
                elif region_from["provider"] == Provider.INTEGRATION_TEST_PROVIDER.value:
                    latency_distribution = self._integration_test_latency_retriever.get_latency_distribution(
                        region_from, region_to
                    )
                self._latency_distribution_cache[cache_key] = latency_distribution
                return latency_distribution
        except ValueError:
            return []

        return []
