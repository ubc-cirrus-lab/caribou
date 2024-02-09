from multi_x_serverless.routing.models.indexer import Indexer


class Region(Indexer):
    def __init__(self, regions: list[str]) -> None:
        self._value_indices: dict[str, int] = {region: index for index, region in enumerate(regions)}

        super().__init__()
