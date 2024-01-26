from typing import Any

from multi_x_serverless.data_collector.components.performance.performance_exporter import PerformanceExporter
from multi_x_serverless.data_collector.components.performance.performance_retriever import PerformanceRetriever
from multi_x_serverless.data_collector.data_collector import DataCollector


class PerformanceCollector(DataCollector):
    def __init__(self) -> None:
        super().__init__()

        # Perhaps this tables should be imports of the constants.py once we confirm the names
        at_region_table: str = "performance_collector_at_region_table"
        from_to_region_table: str = "performance_collector_from_to_region_table"

        self._data_retriever = PerformanceRetriever()
        self._data_exporter = PerformanceExporter(at_region_table, from_to_region_table, self._data_collector_client)

    def run(self) -> None:
        # TODO (#100): Fill Data Collector Implementations

        # Do required application logic using data from carbon collector
        # Process said data, then return the final data into the exporters
        at_performance_region_data: dict[str, Any] = {}
        from_to_performance_region_data: dict[str, Any] = {}

        self._data_exporter.export_all_data(at_performance_region_data, from_to_performance_region_data)
