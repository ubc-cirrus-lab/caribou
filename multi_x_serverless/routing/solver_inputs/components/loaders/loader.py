from abc import ABC, abstractmethod

import numpy as np

class Loader(ABC):
    def __init__(self):
        self._data: dict = None
    
    @abstractmethod
    def setup(self, *args, **kwargs) -> None:
        # Clear Cache
        # self._execution_matrix = None
        # self._transmission_matrix = None
        raise NotImplementedError

    @abstractmethod
    def retrieve_data(self, *args, **kwargs) -> dict:
        '''
        This function is responsible for retrieving a dictionary representation of the loaded data.
        '''
        raise NotImplementedError
    
    def __str__(self) -> str:
        return f"Source(name={self.__class__.__name__})"

    def __repr__(self) -> str:
        return self.__str__()