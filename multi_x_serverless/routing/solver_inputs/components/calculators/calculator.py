from abc import ABC, abstractmethod


class Calculator(ABC):
    def __init__(self):
        pass

    def __str__(self) -> str:
        return f"Calulator(name={self.__class__.__name__})"

    def __repr__(self) -> str:
        return self.__str__()
