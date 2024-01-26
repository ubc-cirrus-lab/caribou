from typing import Any
from multi_x_serverless.data_collector.components.carbon.carbon_exporter import CarbonExporter
from multi_x_serverless.data_collector.data_collector import DataCollector

class WorkflowCollector(DataCollector):
    def __init__(self) -> None:
        super().__init__()
        self._table_key = "carbon_data"
        self._data_retriever = CarbonCollector()
        self._data_exporter = CarbonExporter()
    
    def run(self) -> None:
        # Do required application logic using data from carbon collector
        # Process said data, then return the final data into the exporters
        at_carbon_region_data: dict[str, Any] = {} 
        from_to_carbon_region_data: dict[tuple[str, str], Any] = {}

        self._data_exporter.export_all_data(at_carbon_region_data, from_to_carbon_region_data)