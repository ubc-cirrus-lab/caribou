import typing
from abc import ABC, abstractmethod


class Source(ABC):
    _data: dict

    def __init__(self) -> None:
        pass

    @abstractmethod
    def setup(self, *args: typing.Any, **kwargs: typing.Any) -> None:
        """
        This function is responsible for loading the data from multiple data_loaders.
        """
        raise NotImplementedError

    @abstractmethod
    def get_value(self, *args: typing.Any, **kwargs: typing.Any) -> typing.Any:  # Doesnt have to be float
        """
        This function is responsible for retrieving a single value from a source.
        """
        raise NotImplementedError

    def __str__(self) -> str:
        return f"Source(name={self.__class__.__name__})"

    def __repr__(self) -> str:
        return self.__str__()
