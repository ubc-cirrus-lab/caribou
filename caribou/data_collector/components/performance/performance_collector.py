from typing import Any

from caribou.common.constants import PERFORMANCE_REGION_TABLE
from caribou.data_collector.components.data_collector import DataCollector
from caribou.data_collector.components.performance.performance_exporter import PerformanceExporter
from caribou.data_collector.components.performance.performance_retriever import PerformanceRetriever


class PerformanceCollector(DataCollector):
    def __init__(self) -> None:
        super().__init__()
        self._data_collector_name: str = "performance_collector"

        at_region_table: str = PERFORMANCE_REGION_TABLE

        self._data_retriever: PerformanceRetriever = PerformanceRetriever(self._data_collector_client)
        self._data_exporter: PerformanceExporter = PerformanceExporter(self._data_collector_client, at_region_table)

    def run(self) -> None:
        # Retrieve available regions
        self._available_region_data = self._data_retriever.retrieve_available_regions()

        runtime_region_data: dict[str, Any] = self._data_retriever.retrieve_runtime_region_data()

        self._data_exporter.export_all_data(runtime_region_data)

        # Updates the timestamp of modified regions
        modified_regions: set[str] = self._data_exporter.get_modified_regions()

        # Important: Regions are stored as provider_region
        self._data_exporter.update_available_region_timestamp(self._data_collector_name, modified_regions)
