from caribou.routing.models.indexer import Indexer


class InstanceIndexer(Indexer):
    def __init__(self, nodes: list[dict]) -> None:
        self._value_indices: dict[str, int] = {node["instance_name"]: index for index, node in enumerate(nodes)}

        super().__init__()
