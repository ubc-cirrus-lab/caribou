from abc import ABC, abstractmethod

from multi_x_serverless.common.models.endpoints import Endpoints


class UpdateChecker(ABC):
    def __init__(self, name):
        self.name = name
        self._endpoints = Endpoints()

    @abstractmethod
    def check(self) -> None:
        raise NotImplementedError()
