from abc import abstractmethod

from multi_x_serverless.routing.models.indexer import Indexer
from multi_x_serverless.routing.solver_inputs.components.data_sources.source import Source


class FromToSource(Source):
    @abstractmethod
    def setup(self, loaded_data: dict, items_to_source: list, indexer: Indexer) -> None:
        """
        This function is responsible for loading the data from multiple data_loaders.
        """
        raise NotImplementedError

    def get_value(self, data_name: str, from_index: int, to_index: int) -> float:
        # Doesnt have to be float, data_sources do not enforce the type of
        # data they contain to be any specific type, as doing so limit their flexibility.
        """
        This function is responsible for retrieving a single value from a source.
        """
        return self._data[from_index][to_index][data_name]
