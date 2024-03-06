from abc import ABC, abstractmethod
from typing import Any


class LatencyRetriever(ABC):
    @abstractmethod
    def get_latency_distribution(self, region_from: dict[str, Any], region_to: dict[str, Any]) -> list[float]:
        raise NotImplementedError
