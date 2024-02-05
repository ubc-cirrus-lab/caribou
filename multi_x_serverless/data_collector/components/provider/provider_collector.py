from typing import Any

from multi_x_serverless.common.constants import PROVIDER_REGION_TABLE, PROVIDER_TABLE
from multi_x_serverless.data_collector.components.data_collector import DataCollector
from multi_x_serverless.data_collector.components.provider.provider_exporter import ProviderExporter
from multi_x_serverless.data_collector.components.provider.provider_retriever import ProviderRetriever


class ProviderCollector(DataCollector):
    def __init__(self) -> None:
        super().__init__()
        self._data_collector_name: str = "provider_collector"

        provider_region_table: str = PROVIDER_REGION_TABLE
        provider_table: str = PROVIDER_TABLE

        self._data_retriever: ProviderRetriever = ProviderRetriever(self._data_collector_client)
        self._data_exporter: ProviderExporter = ProviderExporter(
            self._data_collector_client,
            provider_region_table,
            provider_table,
        )

    def run(self) -> None:
        # Retrieve and export available regions
        available_region_data = self._data_retriever.retrieve_available_regions()
        self._data_exporter.export_available_region_table(available_region_data)

        provider_region_data: dict[str, Any] = self._data_retriever.retrieve_provider_region_data()

        # TODO (#27): Implement free tier data collection
        provider_data: dict[str, Any] = {}

        self._data_exporter.export_all_data(provider_region_data, provider_data)

        # Updates the timestamp of modified regions
        modified_regions: set[str] = self._data_exporter.get_modified_regions()

        self._data_exporter.update_available_region_timestamp(self._data_collector_name, modified_regions)


if __name__ == "__main__":
    provider_collector = ProviderCollector()
    provider_collector.run()
