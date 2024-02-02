from abc import ABC, abstractmethod
from typing import Any

from multi_x_serverless.common.models.endpoints import Endpoints
from multi_x_serverless.common.models.remote_client.remote_client import RemoteClient
from multi_x_serverless.data_collector.components.data_exporter import DataExporter
from multi_x_serverless.data_collector.components.data_retriever import DataRetriever


class DataCollector(ABC):
    _data_retriever: DataRetriever
    _data_exporter: DataExporter
    _data_table: str
    _data_collector_name: str

    @abstractmethod
    def __init__(self) -> None:
        self._data_collector_client: RemoteClient = Endpoints().get_data_collector_client()
        self._available_region_data: dict[str, dict[str, Any]] = {}

    @abstractmethod
    def run(self) -> None:
        raise NotImplementedError
