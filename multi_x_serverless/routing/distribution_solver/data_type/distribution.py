from typing import TypeVar
import numpy as np

T = TypeVar('T', bound='Distribution')
class Distribution:
    _samples: np.array[float]
    _max_sample_size: int

    def __init__(self, samples: np.array[float], max_sample_size: int = 1000):
        self._samples = samples
        self._max_sample_size = max_sample_size

    def get_combined_merged_distribution(self, parent_distributions: list[T], max_sample_size: int = -1) -> T:
        max_sample_size = self._consider_sample_size(max_sample_size)

        # Here we should combine the distributions in a merged manner
        # Current strategy: Find the longest (maximum) runtime across these arrays for each corresponding index
        # Perhaps we should consider more sophisticated methods in the future

        # First we must extract the samples from the parent distributions
        parent_runtimes = [distribution._samples for distribution in parent_distributions]

        # Then we have the find the minimum sample size of those runtimes and
        # subsample the longer ones such that they are all the same size
        min_sample_size = min([len(samples) for samples in parent_runtimes])

        # Then since they are all in the same size we can convert them to a numpy array
        parent_runtimes = np.array([self._downsample(samples, min_sample_size) for samples in parent_runtimes])

        # Finally we can find the maximum runtime for each index using np.maximum.reduce
        max_runtimes = np.maximum.reduce(parent_runtimes)

        # Return the downsampled version of the max runtimes and convert it to a Distribution object
        return T(self._downsample(max_runtimes, max_sample_size), max_sample_size)

    def _combine_sequential(self, distribution1: T, distribution2: T, max_sample_size: int = -1) -> T:
        max_sample_size = self._consider_sample_size(max_sample_size)

        # Here we should combine the two distributions in a sequential manner
        # Current strategy: Effectively calculates the total runtime for every possible pair of addition operations from the two sequences.
        combined_samples = np.add.outer(distribution1._samples, distribution2._samples).flatten()

        # Return the downsampled version of the combined samples and convert it to a Distribution object
        return T(self._downsample(combined_samples, max_sample_size), max_sample_size)

    def _downsample(self, samples: np.array[float], max_sample_size: int = -1) -> np.array[float]:
        max_sample_size = self._consider_sample_size(max_sample_size)

        if len(samples) <= max_sample_size:
            return samples

        # Here we should downsample the samples to the max_sample_size
        # Current strategy: Perform simple random sampling without replacement, correctly handling the dataset size
        # Perhaps we should consider more sophisticated methods in the future
        return np.random.choice(samples, size=max_sample_size, replace=False)

    def _consider_sample_size(self, sample_size: int) -> int:
        if sample_size <= 0: # Default sample sizes
            sample_size = self._max_sample_size
        
        return sample_size
    
    def __add__(self, other: T) -> T:
        if isinstance(other, T):
            return self._combine_sequential(self, other)
        else:
            raise TypeError("Unsupported operand type for +: '{}' and '{}'".format(
                type(self).__name__, type(other).__name__))