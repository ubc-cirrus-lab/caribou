from abc import abstractmethod
from typing import Any, Optional

from multi_x_serverless.routing.models.indexer import Indexer
from multi_x_serverless.routing.solver_inputs.components.data_sources.source import Source


class AtSource(Source):
    @abstractmethod
    def setup(
        self, loaded_data: dict, items_to_source: list, indexer: Indexer, configurations: Optional[list[dict]] = None
    ) -> None:
        """
        This function is responsible for loading the data from multiple data_loaders.
        """
        raise NotImplementedError

    def get_value(self, data_name: str, index: int) -> Any:
        # Doesnt have to be float, data_sources do not enforce the type of
        # data they contain to be any specific type, as doing so limit their flexibility.
        """
        This function is responsible for retrieving a single value from a source.
        """
        return self._data[index][data_name]
