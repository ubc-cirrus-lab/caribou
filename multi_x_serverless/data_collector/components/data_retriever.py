from abc import ABC, abstractmethod
from typing import Any

class DataRetriever(ABC):
    @abstractmethod
    def collect_data(self) -> dict[str, Any]:
        """
        Collects data from the data source

        Returns:
            Dict[str, Any]: data collected from the data source
        """
        pass