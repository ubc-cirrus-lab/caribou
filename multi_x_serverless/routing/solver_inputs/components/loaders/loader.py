from abc import ABC
from typing import Any


class Loader(ABC):
    _data: dict[str, Any] = {}

    def retrieve_data(self) -> dict:
        """
        This function is responsible for retrieving a dictionary representation of the loaded data.
        """
        # Throw error if data has not been loaded
        if self._data is None:
            raise ValueError("Data has not been loaded yet. Please call the setup function first.")

        return self._data

    def __str__(self) -> str:
        return f"Source(name={self.__class__.__name__})"

    def __repr__(self) -> str:
        return self.__str__()
