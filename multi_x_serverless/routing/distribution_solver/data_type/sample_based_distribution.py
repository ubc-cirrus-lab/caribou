from typing import TypeVar, cast

import numpy as np

from multi_x_serverless.routing.distribution_solver.data_type.distribution import Distribution

T = TypeVar("T", bound="Distribution")


class SampleBasedDistribution(Distribution):
    _max_sample_size: int

    def __init__(self, samples: np.ndarray = np.zeros(1, dtype=float), max_sample_size: int = 100):
        super().__init__()
        samples.sort()
        self._samples: np.ndarray = samples
        self._non_zero_samples: np.ndarray = samples[samples != 0.0]
        self._max_sample_size = max_sample_size

    def get_merged_distribution(self, parent_distributions: list[T]) -> T:
        for distribution in parent_distributions:
            if not isinstance(distribution, SampleBasedDistribution):
                raise TypeError(
                    f"Expected all distributions to be of type {SampleBasedDistribution.__name__}, "
                    f"but got {type(distribution).__name__}"
                )

        # Here we should combine the distributions in a merged manner
        # Current strategy: Find the longest (maximum) runtime across these arrays for each corresponding index
        # Perhaps we should consider more sophisticated methods in the future

        # First we must extract the samples from the parent distributions
        parent_runtimes = [distribution.get_samples() for distribution in parent_distributions]

        # Then we have the find the minimum sample size of those runtimes and
        # subsample the longer ones such that they are all the same size
        min_sample_size = min(len(samples) for samples in parent_runtimes)

        # Then since they are all in the same size we can convert them to a numpy array
        parent_runtimes = np.array([self._downsample(samples, min_sample_size) for samples in parent_runtimes])

        # Finally we can find the maximum runtime for each index using np.maximum.reduce
        max_runtimes = np.maximum.reduce(parent_runtimes)

        # Return the downsampled version of the max runtimes and convert it to a Distribution object
        return cast(
            T, SampleBasedDistribution(self._downsample(max_runtimes, self._max_sample_size), self._max_sample_size)
        )

    def _combine_sequential(self, distribution1: T, distribution2: T) -> T:
        if not isinstance(distribution1, SampleBasedDistribution):
            raise TypeError(
                f"Expected distribution1 to be of type {SampleBasedDistribution.__name__}, "
                f"but got {type(distribution1).__name__}"
            )
        if not isinstance(distribution2, SampleBasedDistribution):
            raise TypeError(
                f"Expected distribution2 to be of type {SampleBasedDistribution.__name__}, "
                f"but got {type(distribution2).__name__}"
            )

        # Here we should combine the two distributions in a sequential manner
        # Current strategy: Effectively calculates the total runtime for every
        # possible pair of addition operations from the two sequences.
        combined_samples = np.add.outer(distribution1.get_samples(), distribution2.get_samples()).flatten()

        # Return the downsampled version of the combined samples and convert it to a Distribution object
        return cast(
            T, SampleBasedDistribution(self._downsample(combined_samples, self._max_sample_size), self._max_sample_size)
        )

    def _downsample(self, samples: np.ndarray, max_sample_size: int = -1) -> np.ndarray:
        max_sample_size = self._consider_sample_size(max_sample_size)

        if len(samples) <= max_sample_size:
            return samples

        # Here we should downsample the samples to the max_sample_size
        # Current strategy: Perform simple random sampling without replacement, correctly handling the dataset size
        # Perhaps we should consider more sophisticated methods in the future
        return np.random.choice(samples, size=max_sample_size, replace=False)

    def _consider_sample_size(self, sample_size: int) -> int:
        if sample_size <= 0:  # Default sample sizes
            sample_size = self._max_sample_size

        return sample_size

    def __add__(self, other: T) -> T:
        if isinstance(other, SampleBasedDistribution):
            return cast(T, self._combine_sequential(self, other))

        raise TypeError(f"Unsupported operand type for +: '{type(self).__name__}' and '{type(other).__name__}'")

    def get_average(self, ignore_zeros: bool) -> float:
        samples = self._samples if not ignore_zeros else self._non_zero_samples
        if len(samples) == 0:
            return 0.0
        return float(np.mean(samples))

    def get_median(self, ignore_zeros: bool) -> float:
        samples = self._samples if not ignore_zeros else self._non_zero_samples
        if len(samples) == 0:
            return 0.0
        return float(np.median(samples))

    def get_tail_latency(self, ignore_zeros: bool) -> float:
        return self.get_percentile(self._tail_latency_threshold, ignore_zeros)

    def get_min(self, ignore_zeros: bool) -> float:
        samples = self._samples if not ignore_zeros else self._non_zero_samples
        if len(samples) == 0:
            return 0.0
        return float(np.min(samples))

    def get_max(self, ignore_zeros: bool) -> float:
        samples = self._samples if not ignore_zeros else self._non_zero_samples
        if len(samples) == 0:
            return 0.0
        return float(np.max(samples))

    def get_percentile(self, percentile: int, ignore_zeros: bool) -> float:
        samples = self._samples if not ignore_zeros else self._non_zero_samples
        if len(samples) == 0:
            return 0.0
        return float(np.percentile(samples, percentile))
