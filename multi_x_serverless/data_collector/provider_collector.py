from typing import Any

from multi_x_serverless.data_collector.components.provider.provider_exporter import ProviderExporter
from multi_x_serverless.data_collector.components.provider.provider_retriever import ProviderRetriever
from multi_x_serverless.data_collector.data_collector import DataCollector


class ProviderCollector(DataCollector):
    def __init__(self) -> None:
        super().__init__()

        # Perhaps this tables should be imports of the constants.py once we confirm the names
        available_region_table: str = "available_region_table"
        at_region_table: str = "provider_collector_at_region_table"
        from_to_region_table: str = "provider_collector_from_to_region_table"

        self._data_retriever = ProviderRetriever()
        self._data_exporter = ProviderExporter(
            self._data_collector_client, available_region_table, at_region_table, from_to_region_table
        )

    def run(self) -> None:
        # TODO (#100): Fill Data Collector Implementations

        # Do required application logic using data from carbon collector
        # Process said data, then return the final data into the exporters
        at_provider_region_data: dict[str, Any] = {}
        from_to_provider_region_data: dict[str, Any] = {}

        self._data_exporter.export_all_data(at_provider_region_data, from_to_provider_region_data)
