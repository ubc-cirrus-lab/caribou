from .at.instance import InstanceSource
from .at.region import RegionSource
from .from_to.instance import InstanceToInstanceSource
from .from_to.region import RegionToRegionSource

# Indexers
from ....models.indexer import Indexer

class DataSourceManager:
    def __init__(self):
        super().__init__()

        # initialize components
        self._instance_source = InstanceSource()
        self._region_source = RegionSource()
        self._instance_to_instance_source = InstanceToInstanceSource()
        self._region_to_region_source = RegionToRegionSource()

    def setup(self, loaded_data: dict, instance_configuration: dict, regions: list[(str, str)], instances: list[str], regions_indexer: Indexer, instance_indexer: Indexer) -> None:
        # Propagate loaded data to data sources
        self._instance_source.setup(loaded_data, instance_configuration, instances, instance_indexer)
        self._region_source.setup(loaded_data, regions, regions_indexer)

        self._instance_to_instance_source.setup(loaded_data, instances, instance_indexer)
        self._region_to_region_source.setup(loaded_data, regions, regions_indexer)
    
    def get_region_to_region_data(self, data_name: str, from_region_index: int, to_region_index: int): # Result type might not necessarily be float
        return self._region_to_region_source.get_value(data_name, from_region_index, to_region_index)

    def get_instance_to_instance_data(self, data_name: str, from_instance_index: int, to_instance_index: int): # Result type might not necessarily be float
        return self._instance_to_instance_source.get_value(data_name, from_instance_index, to_instance_index)
    
    def get_region_data(self, data_name: str, region_index: int): # Result type might not necessarily be float
        return self._region_source.get_value(data_name, region_index)
    
    def get_instance_data(self, data_name: str, instance_index: int): # Result type might not necessarily be float
        return self._instance_source.get_value(data_name, instance_index)