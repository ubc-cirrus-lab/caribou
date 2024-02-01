from typing import Any

from multi_x_serverless.common.provider import Provider
from multi_x_serverless.data_collector.components.data_retriever import DataRetriever
from multi_x_serverless.data_collector.utils.aws_latency_retriever import AWSLatencyRetriever
from multi_x_serverless.deployment.common.remote_client.remote_client import RemoteClient


class PerformanceRetriever(DataRetriever):
    def __init__(self, client: RemoteClient) -> None:
        super().__init__(client)
        self._aws_latency_retriever = AWSLatencyRetriever()
        self._modified_regions: set[str] = set()

    def retrieve_runtime_region_data(self) -> dict[str, dict[str, Any]]:
        result_dict: dict[str, dict[str, Any]] = {}
        for region_key, available_region in self._available_regions.items():
            transmission_latency_dict = {}

            for region_key_to, available_region_to in self._available_regions.items():
                transmission_latency_dict[region_key_to] = {
                    "transmission_latency": self._get_total_latency(available_region, available_region_to),
                    "unit": "ms",
                }

            result_dict[region_key] = {
                "relative_performance": 1,  # Current assumption is that all regions have the same performance (At least within the same provider)
                "transmission_latency": transmission_latency_dict,
            }
        return result_dict

    def _get_total_latency(self, region_from: dict[str, Any], region_to: dict[str, Any]) -> float:
        if region_from["provider"] == region_to["provider"] and region_from["provider"] == Provider.AWS.value:
            return self._aws_latency_retriever.get_latency(region_from, region_to)

        return 0.0  # Default value, maybe a better default or an error message will be desired
