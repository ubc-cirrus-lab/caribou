from abc import ABC, abstractmethod
from typing import Any

class DataProcessor(ABC):
    @abstractmethod
    def process_data(self) -> dict[str, Any]:
        """
        Processes data from the data source

        Returns:
            Dict[str, Any]: data processed from the data source
        """
        pass