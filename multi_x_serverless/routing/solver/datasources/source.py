import numpy as np


class Source:
    def __init__(self, name: str, type: str, data: np.ndarray):
        self.name = name
        self.type = type
        self.data = data

    def __str__(self) -> str:
        return f"Source(name={self.name}, type={self.type}, data={self.data})"

    def __repr__(self) -> str:
        return self.__str__()

    def get_execution_matrix(self, regions: np.ndarray, functions: np.ndarray) -> np.ndarray:
        raise NotImplementedError

    def get_transmission_matrix(self, regions: np.ndarray) -> np.ndarray:
        raise NotImplementedError
