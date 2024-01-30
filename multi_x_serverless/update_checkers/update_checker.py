from abc import ABC, abstractmethod


class UpdateChecker(ABC):
    def __init__(self, name):
        self.name = name

    @abstractmethod
    def check(self):
        pass
