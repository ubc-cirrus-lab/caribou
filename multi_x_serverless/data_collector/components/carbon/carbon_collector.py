from typing import Any

from multi_x_serverless.common.constants import CARBON_AT_REGION_TABLE, CARBON_FROM_TO_REGION_TABLE
from multi_x_serverless.data_collector.components.carbon.carbon_exporter import CarbonExporter
from multi_x_serverless.data_collector.components.carbon.carbon_retriever import CarbonRetriever
from multi_x_serverless.data_collector.components.data_collector import DataCollector


class CarbonCollector(DataCollector):
    def __init__(self) -> None:
        super().__init__()
        self._data_collector_name: str = "carbon_collector"

        at_region_table: str = CARBON_AT_REGION_TABLE
        from_to_region_table: str = CARBON_FROM_TO_REGION_TABLE

        self._data_retriever = CarbonRetriever(self._data_collector_client)
        self._data_exporter = CarbonExporter(self._data_collector_client, at_region_table, from_to_region_table)

    def run(self) -> None:
        # Retrieve available regions
        available_region_data = self._data_retriever.retrieve_available_regions()
        # TODO (#100): Fill Data Collector Implementations

        # Do required application logic using data from carbon retriever
        # Process said data, then return the final data into the exporters
        at_carbon_region_data: dict[str, Any] = {}
        from_to_carbon_region_data: dict[str, Any] = {}

        self._data_exporter.export_all_data(at_carbon_region_data, from_to_carbon_region_data)

        # Updates the timestamp of modified regions
        modified_regions: list[str] = []  # Regions we are updating in this collector
        # Important: Regions are stored as provider_region
        self._data_exporter.update_available_region_timestamp(self._data_collector_name, modified_regions)
