from copy import deepcopy

from caribou.deployment_solver.models.indexer import Indexer


class RegionIndexer(Indexer):
    def __init__(self, regions: list[str]) -> None:
        self._value_indices: dict[str, int] = {region: index for index, region in enumerate(regions)}
        self._regions = deepcopy(regions)
        super().__init__()

    def get_regions(self) -> list[str]:
        return self._regions
