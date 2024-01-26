from typing import Any

from multi_x_serverless.data_collector.components.carbon.carbon_exporter import CarbonExporter
from multi_x_serverless.data_collector.components.carbon.carbon_retriever import CarbonRetriever
from multi_x_serverless.data_collector.data_collector import DataCollector


class CarbonCollector(DataCollector):
    def __init__(self) -> None:
        super().__init__()

        # Perhaps this tables should be imports of the constants.py once we confirm the names
        available_region_table: str = "available_region_table"
        at_region_table: str = "carbon_collector_at_region_table"
        from_to_region_table: str = "carbon_collector_from_to_region_table"

        self._data_retriever = CarbonRetriever()
        self._data_exporter = CarbonExporter(
            self._data_collector_client, available_region_table, at_region_table, from_to_region_table
        )

    def run(self) -> None:
        # TODO (#100): Fill Data Collector Implementations

        # Do required application logic using data from carbon retriever
        # Process said data, then return the final data into the exporters
        at_carbon_region_data: dict[str, Any] = {}
        from_to_carbon_region_data: dict[str, Any] = {}

        self._data_exporter.export_all_data(at_carbon_region_data, from_to_carbon_region_data)
