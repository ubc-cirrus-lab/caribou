from .source import Source

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

    def setup(self, loaded_data: dict, regions: list[(str, str)], instances: list[str], regions_indexer: Indexer, instance_indexer: Indexer) -> bool:
        # Propagate loaded data to data sources
        self._instance_source.setup(loaded_data, regions, regions_indexer)
        self._region_source.setup(loaded_data, regions, regions_indexer)

        self._instance_to_instance_source.setup(loaded_data, regions, instances, regions_indexer, instance_indexer)
        self._region_to_region_source.setup(loaded_data, regions, instances, regions_indexer, instance_indexer)

        return False
    
    def retrieve_data(self) -> dict:
        return self._data