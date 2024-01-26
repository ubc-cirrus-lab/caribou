from typing import Any

from multi_x_serverless.data_collector.components.provider.provider_exporter import ProviderExporter
from multi_x_serverless.data_collector.components.provider.provider_retriever import ProviderRetriever
from multi_x_serverless.data_collector.data_collector import DataCollector


class ProviderCollector(DataCollector):
    def __init__(self) -> None:
        super().__init__()

        # Perhaps this tables should be imports of the constants.py once we confirm the names
        at_region_table: str = "provider_collector_at_region_table"
        from_to_region_table: str = "provider_collector_from_to_region_table"

        self._data_retriever = ProviderRetriever()
        self._data_exporter = ProviderExporter(at_region_table, from_to_region_table, self._data_collector_client)

    def run(self) -> None:
        # Do required application logic using data from carbon collector
        # Process said data, then return the final data into the exporters
        at_provider_region_data: dict[str, Any] = {}
        from_to_provider_region_data: dict[tuple[str, str], Any] = {}

        self._data_exporter.export_all_data(at_provider_region_data, from_to_provider_region_data)
