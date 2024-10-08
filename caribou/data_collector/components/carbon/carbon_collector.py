from typing import Any

from caribou.common.constants import CARBON_REGION_TABLE
from caribou.data_collector.components.carbon.carbon_exporter import CarbonExporter
from caribou.data_collector.components.carbon.carbon_retriever import CarbonRetriever
from caribou.data_collector.components.data_collector import DataCollector


class CarbonCollector(DataCollector):
    def __init__(self) -> None:
        super().__init__()
        self._data_collector_name: str = "carbon_collector"

        carbon_region_table: str = CARBON_REGION_TABLE

        self._data_retriever: CarbonRetriever = CarbonRetriever(self._data_collector_client)
        self._data_exporter: CarbonExporter = CarbonExporter(self._data_collector_client, carbon_region_table)

    def run(self) -> None:
        # Retrieve available regions
        self._available_region_data = self._data_retriever.retrieve_available_regions()

        carbon_region_data: dict[str, Any] = self._data_retriever.retrieve_carbon_region_data()

        self._data_exporter.export_all_data(carbon_region_data)

        # Updates the timestamp of modified regions
        modified_regions: set[str] = self._data_exporter.get_modified_regions()
        # Important: Regions are stored as provider_region
        self._data_exporter.update_available_region_timestamp(self._data_collector_name, modified_regions)


if __name__ == "__main__":
    carbon_collector = CarbonCollector()
    carbon_collector.run()
