from abc import ABC, abstractmethod

class Calculator(ABC):
    def __init__(self):
        pass

    # @abstractmethod
    # def calculate_execution(self, instance_index: int, region_index: int) -> float:
    #     raise NotImplementedError
    
    # @abstractmethod
    # def calculate_transmission(self, from_instance_index: int, to_instance_index: int, from_region_index: int, to_region_index: int) -> float:
    #     raise NotImplementedError

    def __str__(self) -> str:
        return f"Calulator(name={self.__class__.__name__})"

    def __repr__(self) -> str:
        return self.__str__()