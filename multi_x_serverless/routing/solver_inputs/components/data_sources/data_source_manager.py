# Indexers
import typing

from multi_x_serverless.routing.models.indexer import Indexer
from multi_x_serverless.routing.solver_inputs.components.data_sources.at.instance import InstanceSource
from multi_x_serverless.routing.solver_inputs.components.data_sources.at.region import RegionSource
from multi_x_serverless.routing.solver_inputs.components.data_sources.from_to.instance import InstanceToInstanceSource
from multi_x_serverless.routing.solver_inputs.components.data_sources.from_to.region import RegionToRegionSource


class DataSourceManager:
    def __init__(self) -> None:
        # initialize components
        self._instance_source = InstanceSource()
        self._region_source = RegionSource()
        self._instance_to_instance_source = InstanceToInstanceSource()
        self._region_to_region_source = RegionToRegionSource()

    def setup(
        self,
        loaded_data: dict,
        instance_configuration: list[dict],
        regions: list[tuple[str, str]],
        instances: list[str],
        regions_indexer: Indexer,
        instance_indexer: Indexer,
    ) -> None:
        # Propagate loaded data to data sources
        self._instance_source.setup(loaded_data, instance_configuration, instances, instance_indexer)
        self._region_source.setup(loaded_data, regions, regions_indexer)

        self._instance_to_instance_source.setup(loaded_data, instances, instance_indexer)
        self._region_to_region_source.setup(loaded_data, regions, regions_indexer)

    def get_region_to_region_data(self, data_name: str, from_region_index: int, to_region_index: int) -> typing.Any:
        # Result type might not necessarily be float
        # Since region_to_regions_source contains non-float data
        return self._region_to_region_source.get_value(data_name, from_region_index, to_region_index)

    def get_instance_to_instance_data(self, data_name: str, from_instance_index: int, to_instance_index: int) -> float:
        return self._instance_to_instance_source.get_value(data_name, from_instance_index, to_instance_index)

    def get_region_data(self, data_name: str, region_index: int) -> typing.Any:
        # Result type might not necessarily be float
        # Since region_source contains non-float data
        return self._region_source.get_value(data_name, region_index)

    def get_instance_data(self, data_name: str, instance_index: int) -> typing.Any:
        # Result type might not necessarily be float
        # Since instance_source contains non-float data
        return self._instance_source.get_value(data_name, instance_index)
