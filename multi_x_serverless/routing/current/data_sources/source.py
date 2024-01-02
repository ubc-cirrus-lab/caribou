import numpy as np


class Source:
    def __init__(self, name: str, type: str, data: np.ndarray, regions: np.ndarray, functions: np.ndarray):
        self._name = name
        self._type = type
        self._data = data
        self._regions = regions
        self._functions = functions

    def __str__(self) -> str:
        return f"Source(name={self._name}, type={self._type}, data={self._data})"

    def __repr__(self) -> str:
        return self.__str__()

    def get_execution_matrix(self) -> np.ndarray:
        raise NotImplementedError

    def get_transmission_matrix(self) -> np.ndarray:
        raise NotImplementedError

    def get_name(self) -> str:
        return self._name

    def get_type(self) -> str:
        return self._type
