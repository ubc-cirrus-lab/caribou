import json
from abc import ABC, abstractmethod
from typing import Any

from multi_x_serverless.common.models.endpoints import Endpoints

from multi_x_serverless.data_collector.components.data_retriever import DataRetriever
from multi_x_serverless.data_collector.components.data_exporter import DataExporter

class DataCollector(ABC):
    _data_retriever: DataRetriever
    _data_exporter: DataExporter
    _data_table: str
    
    @abstractmethod
    def __init__(self) -> None:
        self._data_collector_client = Endpoints().get_data_collector_client()

    @abstractmethod
    def run(self) -> None:
        raise NotImplementedError