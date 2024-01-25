import json
from abc import ABC, abstractmethod
from typing import Any

from multi_x_serverless.common.models.endpoints import Endpoints

from multi_x_serverless.data_collectors.components.data_retriever import DataRetriever
from multi_x_serverless.data_collectors.components.data_processor import DataProcessor
from multi_x_serverless.data_collectors.components.data_exporter import DataExporter

class DataCollector(ABC):
    _data_retriever: DataRetriever
    _data_processor: DataProcessor
    _data_exporter: DataExporter
    _data_table: str
    
    @abstractmethod
    def __init__(self) -> None:
        self._data_collector_client = Endpoints().get_data_collector_client()

    def run(self) -> None:
        retrieved_data = self._data_retriever.collect_data()
        processed_data = self._data_processor.process_data(retrieved_data)
        self._data_exporter.export_data(processed_data)