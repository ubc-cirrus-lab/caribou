# Loader
from multi_x_serverless.routing.distribution_solver.data_type.distribution import Distribution
from multi_x_serverless.routing.models.dag import DAG
from multi_x_serverless.routing.models.region import Region


class DistributionInputManager:  # pylint: disable=too-many-instance-attributes
    _available_regions: list[str]
    _region_indexer: Region
    _instance_indexer: DAG

    def __init__(self) -> None:
        super().__init__()

    def setup(self, regions_indexer: Region, instance_indexer: DAG) -> None:
        self._region_indexer = regions_indexer
        self._instance_indexer = instance_indexer

    def get_execution_cost_carbon_runtime_distribution(
        self, instance_index: int, region_index: int
    ) -> list[Distribution]:
        test_sum = instance_index + region_index
        print(test_sum)
        return [Distribution(), Distribution(), Distribution()]

    def get_transmission_cost_carbon_runtime_distribution(
        self, from_instance_index: int, to_instance_index: int, from_region_index: int, to_region_index: int
    ) -> list[Distribution]:
        test_sum = from_instance_index + to_instance_index + from_region_index + to_region_index
        print(test_sum)
        return [Distribution(), Distribution(), Distribution()]

    def get_all_regions(self) -> list[str]:
        return self._available_regions
