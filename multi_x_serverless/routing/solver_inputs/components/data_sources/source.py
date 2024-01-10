from abc import ABC, abstractmethod

# Indexers
from ....models.indexer import Indexer

class Source(ABC):
    def __init__(self):
        self._data: dict = None
        pass
    
    @abstractmethod
    def setup(self, loaded_data: dict, regions_indexer: Indexer, instance_indexer: Indexer) -> bool:
        '''
        This function is responsible for loading the data from multiple data_loaders.
        '''
        # Clear Cache
        raise NotImplementedError

    @abstractmethod
    def get_value(self, *args, **kwargs) -> float:
        '''
        This function is responsible for retrieving a single value from a source.
        '''
        raise NotImplementedError

    def __str__(self) -> str:
        return f"Source(name={self.__class__.__name__})"

    def __repr__(self) -> str:
        return self.__str__()