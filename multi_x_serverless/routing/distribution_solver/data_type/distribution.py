from abc import ABC, abstractmethod
from typing import TypeVar

import numpy as np

T = TypeVar("T", bound="Distribution")


class Distribution(ABC):
    _samples: np.ndarray

    def __init__(self) -> None:
        self._tail_latency_threshold = 95  # Would become an environemntal variable later

    @abstractmethod
    def get_merged_distribution(self, parent_distributions: list[T]) -> T:
        raise NotImplementedError

    @abstractmethod
    def _combine_sequential(self, distribution1: T, distribution2: T) -> T:
        raise NotImplementedError

    @abstractmethod
    def __add__(self, other: T) -> T:
        raise NotImplementedError

    @abstractmethod
    def get_average(self, ignore_zeros: bool) -> float:
        raise NotImplementedError

    @abstractmethod
    def get_median(self, ignore_zeros: bool) -> float:
        raise NotImplementedError

    @abstractmethod
    def get_tail_latency(self, ignore_zeros: bool) -> float:
        raise NotImplementedError

    @abstractmethod
    def get_min(self, ignore_zeros: bool) -> float:
        raise NotImplementedError

    @abstractmethod
    def get_max(self, ignore_zeros: bool) -> float:
        raise NotImplementedError

    @abstractmethod
    def get_percentile(self, percentile: int, ignore_zeros: bool) -> float:
        raise NotImplementedError

    def get_samples(self) -> np.ndarray:
        return self._samples
