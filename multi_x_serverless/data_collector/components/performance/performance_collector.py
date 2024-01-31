from typing import Any

from multi_x_serverless.common.constants import PERFORMANCE_REGION_TABLE
from multi_x_serverless.data_collector.components.data_collector import DataCollector
from multi_x_serverless.data_collector.components.performance.performance_exporter import PerformanceExporter
from multi_x_serverless.data_collector.components.performance.performance_retriever import PerformanceRetriever


class PerformanceCollector(DataCollector):
    def __init__(self) -> None:
        super().__init__()
        self._data_collector_name: str = "performance_collector"

        at_region_table: str = PERFORMANCE_REGION_TABLE

        self._data_retriever = PerformanceRetriever(self._data_collector_client)
        self._data_exporter = PerformanceExporter(self._data_collector_client, at_region_table)

    def run(self) -> None:
        # Retrieve available regions
        available_region_data = self._data_retriever.retrieve_available_regions()
        # TODO (#100): Fill Data Collector Implementations

        # Do required application logic using data from carbon collector
        # Process said data, then return the final data into the exporters
        at_performance_region_data: dict[str, Any] = {}
        from_to_performance_region_data: dict[str, Any] = {}

        self._data_exporter.export_all_data(at_performance_region_data, from_to_performance_region_data)

        # Updates the timestamp of modified regions
        modified_regions: set[str] = set()  # Regions we are updating in this collector
        # Important: Regions are stored as provider_region
        self._data_exporter.update_available_region_timestamp(self._data_collector_name, modified_regions)
