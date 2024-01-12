from abc import ABC, abstractmethod

import numpy as np


class Source(ABC):
    def __init__(self):
        # self._execution_matrix: np.ndarray = None
        # self._transmission_matrix: np.ndarray = None
        pass

    @abstractmethod
    def setup(self, *args, **kwargs) -> None:
        # Clear Cache
        # self._execution_matrix = None
        # self._transmission_matrix = None
        raise NotImplementedError

    @abstractmethod
    def load_database(self, *args, **kwargs) -> dict:
        """
        This function is responsible for loading the data from the database and converting it into a dictionary.
        """
        raise NotImplementedError

    # def get_execution_matrix(self) -> np.ndarray:
    #     return self._execution_matrix

    # def get_transmission_matrix(self) -> np.ndarray:
    #     return self._transmission_matrix

    @abstractmethod
    def load_database(self, *args, **kwargs) -> dict:
        """
        This function is responsible for loading the data from the database and converting it into a dictionary.
        """
        raise NotImplementedError

    def __str__(self) -> str:
        return f"Source(name={self.__class__.__name__})"

    def __repr__(self) -> str:
        return self.__str__()
