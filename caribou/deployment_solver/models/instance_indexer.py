from copy import deepcopy

from caribou.deployment_solver.models.indexer import Indexer


class InstanceIndexer(Indexer):
    def __init__(self, nodes: list[dict]) -> None:
        self._value_indices: dict[str, int] = {node["instance_name"]: index for index, node in enumerate(nodes)}
        self._nodes = deepcopy(nodes)
        super().__init__()

    def get_nodes(self) -> list[dict]:
        return self._nodes
