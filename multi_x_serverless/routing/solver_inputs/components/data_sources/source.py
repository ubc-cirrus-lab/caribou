from abc import ABC, abstractmethod

# Indexers
from ....models.indexer import Indexer

class Source(ABC):
    def __init__(self):
        self._data: dict = None
        pass
    
    @abstractmethod
    def setup(self, *args, **kwargs) -> None:
        '''
        This function is responsible for loading the data from multiple data_loaders.
        '''
        # Clear Cache
        raise NotImplementedError

    @abstractmethod
    def get_value(self, *args, **kwargs): # Doesnt have to be float
        '''
        This function is responsible for retrieving a single value from a source.
        '''
        raise NotImplementedError

    def __str__(self) -> str:
        return f"Source(name={self.__class__.__name__})"

    def __repr__(self) -> str:
        return self.__str__()