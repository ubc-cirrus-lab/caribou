from typing import TypeVar, cast

import numpy as np

from multi_x_serverless.routing.distribution_solver.data_type.distribution import Distribution

T = TypeVar("T", bound="Distribution")


class SampleBasedDistribution(Distribution):
    _max_sample_size: int

    def __init__(
        self,
        samples: np.ndarray = np.zeros(1, dtype=float),
        probability_of_invocation: float = 1.0,
        max_sample_size: int = 1000,
    ):
        super().__init__()
        # We want to avoid contaiminating this with zero samples
        # Such that we want to do this BEFORE adding zero samples
        self._non_zero_samples: np.ndarray = samples[samples != 0.0]
        self._max_sample_size = max_sample_size
        self._probability_of_invocation = probability_of_invocation

        # We want to add zero samples to the distribution
        self._samples: np.ndarray = self._add_zero_samples(samples, probability_of_invocation)

    def _add_zero_samples(self, samples: np.ndarray, probability_of_invocation: float) -> np.ndarray:
        # Handle the case where we have 0 or 1 probability of invocation
        if probability_of_invocation >= 1.0:
            return samples
        if probability_of_invocation <= 0.001:  # If the probability is too low, we should just return an empty array
            return np.zeros(1, dtype=float)

        num_zeros = int(len(samples) / probability_of_invocation)
        zero_samples = np.zeros(num_zeros, dtype=float)
        final_samples = np.concatenate((samples, zero_samples))
        final_samples.sort()

        # If the final_sample is WAY too big, we should downsample it
        # Else we might just want to preserve the original size
        if len(final_samples) > self._max_sample_size * 5:
            final_samples = self._downsample(final_samples, self._max_sample_size)

        # # print length of new zero samples and non zero samples
        # print(len(final_samples[final_samples <= 0.0]), len(final_samples[final_samples > 0.0]))

        return final_samples

    def get_merged_distribution(self, parent_distributions: list[T]) -> T:
        for distribution in parent_distributions:
            if not isinstance(distribution, SampleBasedDistribution):
                raise TypeError(
                    f"Expected all distributions to be of type {SampleBasedDistribution.__name__}, "
                    f"but got {type(distribution).__name__}"
                )

        # First we must extract the samples from the parent distributions
        parent_runtimes = [distribution.get_samples() for distribution in parent_distributions]

        # Then we have to find the maximum sample size of those runtimes and
        # pad the shorter ones with zeros such that they are all the same size
        max_sample_size = max(len(samples) for samples in parent_runtimes)

        # Then since they are all in the same size we can convert them to a numpy array
        parent_runtimes = np.array(
            [self._pad_with_zeros_and_sort(samples, max_sample_size) for samples in parent_runtimes]
        )

        # Finally we can find the maximum runtime for each index using np.maximum.reduce
        max_runtimes = np.maximum.reduce(parent_runtimes)

        # Return the padded version of the max runtimes and convert it to a Distribution object
        return cast(T, SampleBasedDistribution(max_runtimes, self._max_sample_size))

    def _pad_with_zeros_and_sort(self, samples: np.ndarray, max_sample_size: int) -> np.ndarray:
        if len(samples) < max_sample_size:
            padded_array = np.pad(samples, (0, max_sample_size - len(samples)))
            return np.sort(padded_array)
        return np.sort(samples)

    def _alternative_get_merged_distribution(self, parent_distributions: list[T]) -> T:
        for distribution in parent_distributions:
            if not isinstance(distribution, SampleBasedDistribution):
                raise TypeError(
                    f"Expected all distributions to be of type {SampleBasedDistribution.__name__}, "
                    f"but got {type(distribution).__name__}"
                )

        # Here we should combine the distributions in a merged manner
        # Current strategy: Find the maximum runtime across combinations of every two runtimes
        # Perhaps we should consider more sophisticated methods in the future

        # First we must extract the samples from the parent distributions
        parent_runtimes: list[np.ndarray] = [distribution.get_samples() for distribution in parent_distributions]

        # We then go through all the runtimes, take the max between a combination of every two runtimes
        # downsample it then do the same for the next runtime
        for i in range(len(parent_runtimes) - 1):
            parent_runtimes[i + 1] = self._downsample(
                np.maximum.outer(parent_runtimes[i], parent_runtimes[i + 1]).flatten(), self._max_sample_size
            )

        # Return the downsampled version of the max runtimes and convert it to a Distribution object
        return cast(
            T, SampleBasedDistribution(parent_runtimes[-1], self._probability_of_invocation, self._max_sample_size)
        )

    def _alternative_downsample_approach_get_merged_distribution(self, parent_distributions: list[T]) -> T:
        for distribution in parent_distributions:
            if not isinstance(distribution, SampleBasedDistribution):
                raise TypeError(
                    f"Expected all distributions to be of type {SampleBasedDistribution.__name__}, "
                    f"but got {type(distribution).__name__}"
                )

        # Here we should combine the distributions in a merged manner
        # Current strategy: Find the longest (maximum) runtime across these arrays for each corresponding index
        # Perhaps we should consider more sophisticated methods in the future
        # This variation however is distorts the distribution by downsampling the longer runtimes

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
