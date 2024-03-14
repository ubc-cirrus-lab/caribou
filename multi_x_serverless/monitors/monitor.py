from abc import ABC, abstractmethod

from multi_x_serverless.common.models.endpoints import Endpoints


class Monitor(ABC):
    def __init__(self) -> None:
        self._endpoints = Endpoints()

    @abstractmethod
    def check(self) -> None:
        raise NotImplementedError()
