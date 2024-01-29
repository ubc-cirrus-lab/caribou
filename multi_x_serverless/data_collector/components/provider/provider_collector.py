from typing import Any

from multi_x_serverless.data_collector.components.provider.provider_exporter import ProviderExporter
from multi_x_serverless.data_collector.components.provider.provider_retriever import ProviderRetriever
from multi_x_serverless.data_collector.components.data_collector import DataCollector
from multi_x_serverless.common.constants import (
    AVAILABLE_REGIONS_TABLE,
    PROVIDER_AT_REGION_TABLE,
    PROVIDER_FROM_TO_REGION_TABLE,
    PROVIDER_AT_PROVIDER_TABLE,
)


class ProviderCollector(DataCollector):
    def __init__(self) -> None:
        super().__init__()
        self._data_collector_name: str = "provider_collector"

        available_region_table: str = AVAILABLE_REGIONS_TABLE
        at_region_table: str = PROVIDER_AT_REGION_TABLE
        from_to_region_table: str = PROVIDER_FROM_TO_REGION_TABLE
        at_provider_table: str = PROVIDER_AT_PROVIDER_TABLE

        self._data_retriever: ProviderRetriever = ProviderRetriever()
        self._data_exporter: ProviderExporter = ProviderExporter(
            self._data_collector_client,
            available_region_table,
            at_region_table,
            from_to_region_table,
            at_provider_table,
        )

    def run(self) -> None:
        # Retrieve and export available regions
        available_region_data = self._data_retriever.retrieve_available_regions()
        self._data_exporter.export_available_region_table(available_region_data)

        # Do required application logic using data from provider collector
        # Process said data, then return the final data into the exporters
        at_provider_region_data: dict[str, Any] = {}
        from_to_provider_region_data: dict[str, Any] = {}

        self._data_exporter.export_all_data(at_provider_region_data, from_to_provider_region_data)

        # Updates the timestamp of modified regions
        modified_regions: list[str] = []  # Regions we are updating in this collector
        # Important: Regions are stored as provider_region
        self._data_exporter.update_available_region_timestamp(self._data_collector_name, modified_regions)
