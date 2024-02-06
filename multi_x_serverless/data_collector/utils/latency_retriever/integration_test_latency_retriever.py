from typing import Any

from multi_x_serverless.data_collector.utils.latency_retriever.latency_retriever import LatencyRetriever


class IntegrationTestLatencyRetriever(LatencyRetriever):
    def __init__(self) -> None:
        super().__init__()

        self._latency_matrix = [[10, 150, 80, 200], [150, 10, 100, 250], [80, 100, 10, 150], [200, 250, 150, 10]]

        self._code_to_index = {"rivendell": 0, "lothlorien": 1, "anduin": 2, "fangorn": 3}

    def get_latency(self, region_from: dict[str, Any], region_to: dict[str, Any]) -> float:
        region_from_code = region_from["code"]
        region_to_code = region_to["code"]

        return self._latency_matrix[self._code_to_index[region_from_code]][self._code_to_index[region_to_code]]
