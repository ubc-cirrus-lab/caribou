from caribou.deployment_solver.models.indexer import Indexer


class RegionIndexer(Indexer):
    def __init__(self, regions: list[str]) -> None:
        self._value_indices: dict[str, int] = {region: index for index, region in enumerate(regions)}

        super().__init__()
