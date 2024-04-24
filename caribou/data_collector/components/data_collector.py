from abc import ABC, abstractmethod
from typing import Any

from caribou.common.models.endpoints import Endpoints
from caribou.common.models.remote_client.remote_client import RemoteClient
from caribou.data_collector.components.data_exporter import DataExporter
from caribou.data_collector.components.data_retriever import DataRetriever


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
