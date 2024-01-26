from typing import Any

from multi_x_serverless.data_collector.components.performance.performance_exporter import PerformanceExporter
from multi_x_serverless.data_collector.components.performance.performance_retriever import PerformanceRetriever
from multi_x_serverless.data_collector.data_collector import DataCollector


class PerformanceCollector(DataCollector):
    def __init__(self) -> None:
        super().__init__()
        self._data_collector_name: str = "performance_collector"

        # Perhaps this tables should be imports of the constants.py once we confirm the names
        available_region_table: str = "available_region_table"
        at_region_table: str = "performance_collector_at_region_table"
        from_to_region_table: str = "performance_collector_from_to_region_table"

        self._data_retriever = PerformanceRetriever()
        self._data_exporter = PerformanceExporter(
            self._data_collector_client, available_region_table, at_region_table, from_to_region_table
        )

    def run(self) -> None:
        # TODO (#100): Fill Data Collector Implementations

        # Do required application logic using data from carbon collector
        # Process said data, then return the final data into the exporters
        at_performance_region_data: dict[str, Any] = {}
        from_to_performance_region_data: dict[str, Any] = {}

        self._data_exporter.export_all_data(at_performance_region_data, from_to_performance_region_data)

        # Updates the timestamp of modified regions
        modified_regions: list[str] = []  # Regions we are updating in this collector
        self._data_exporter.update_available_region_timestamp(self._data_collector_name, modified_regions)
