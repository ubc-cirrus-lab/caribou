# Loader
from multi_x_serverless.routing.distribution_solver.data_type.distribution import Distribution
from multi_x_serverless.routing.distribution_solver.data_type.sample_based_distribution import SampleBasedDistribution
from multi_x_serverless.routing.models.dag import DAG
from multi_x_serverless.routing.models.region import Region
from multi_x_serverless.routing.workflow_config import WorkflowConfig


class DistributionInputManager:  # pylint: disable=too-many-instance-attributes
    _available_regions: list[str]
    _region_indexer: Region
    _instance_indexer: DAG

    def __init__(self, config: WorkflowConfig, setup_region_viability: bool = True) -> None:
        super().__init__()
        self._config = config

        # Setup the viability loader and load available regions
        if setup_region_viability:
            pass  # Not implemented

    def setup(self, regions_indexer: Region, instance_indexer: DAG) -> None:
        self._region_indexer = regions_indexer
        self._instance_indexer = instance_indexer

    def get_execution_cost_carbon_runtime_distribution(
        self, instance_index: int, region_index: int
    ) -> list[Distribution]:
        test_sum = instance_index + region_index
        print(test_sum)
        return [
            SampleBasedDistribution(),
            SampleBasedDistribution(),
            SampleBasedDistribution(),
        ]

    def get_transmission_cost_carbon_runtime_distribution(
        self, from_instance_index: int, to_instance_index: int, from_region_index: int, to_region_index: int
    ) -> list[Distribution]:
        # If the instance index is not found, return an empty distribution
        if from_instance_index == -1 or to_instance_index == -1:
            return [
                SampleBasedDistribution(),
                SampleBasedDistribution(),
                SampleBasedDistribution(),
            ]

        test_sum = from_region_index + to_region_index
        print(test_sum)

        return [
            SampleBasedDistribution(),
            SampleBasedDistribution(),
            SampleBasedDistribution(),
        ]

    def get_all_regions(self) -> list[str]:
        return self._available_regions
