from abc import ABC, abstractmethod
from typing import Any


class LatencyRetriever(ABC):
    @abstractmethod
    def get_latency(self, region_from: dict[str, Any], region_to: dict[str, Any]) -> float:
        raise NotImplementedError
