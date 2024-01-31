from typing import Any

from multi_x_serverless.common.constants import (
    PROVIDER_TABLE,
    PROVIDER_REGION_TABLE,
    PROVIDER_FROM_TO_REGION_TABLE,
)
from multi_x_serverless.data_collector.components.data_collector import DataCollector
from multi_x_serverless.data_collector.components.provider.provider_exporter import ProviderExporter
from multi_x_serverless.data_collector.components.provider.provider_retriever import ProviderRetriever


class ProviderCollector(DataCollector):
    def __init__(self) -> None:
        super().__init__()
        self._data_collector_name: str = "provider_collector"

        provider_region_table: str = PROVIDER_REGION_TABLE
        provider_table: str = PROVIDER_TABLE

        self._data_retriever: ProviderRetriever = ProviderRetriever()
        self._data_exporter: ProviderExporter = ProviderExporter(
            self._data_collector_client,
            provider_region_table,
            provider_table,
        )

    def run(self) -> None:
        # Retrieve and export available regions
        available_region_data = self._data_retriever.retrieve_available_regions()
        self._data_exporter.export_available_region_table(available_region_data)

        provider_region_data: dict[str, Any] = {}

        # TODO (#27): Implement free tier data collection
        provider_data: dict[str, Any] = {}

        self._data_exporter.export_all_data(provider_region_data, provider_data)

        # Updates the timestamp of modified regions
        modified_regions: set[str] = set()  # Regions we are updating in this collector
        # Important: Regions are stored as provider_region
        self._data_exporter.update_available_region_timestamp(self._data_collector_name, modified_regions)
