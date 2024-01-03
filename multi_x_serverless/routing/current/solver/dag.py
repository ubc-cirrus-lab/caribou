import numpy as np


class DAG:
    def __init__(self, nodes: list[dict]):
        self._nodes: list[dict] = nodes
        self._num_nodes: int = len(nodes)
        self._adj_matrix: np.ndarray = np.zeros((self.num_nodes, self.num_nodes), dtype=int)
        self._node_indices: dict[str, int] = {node["instance_name"]: index for index, node in enumerate(nodes)}

    def add_edge(self, from_node: str, to_node: str) -> None:
        if from_node in self._node_indices and to_node in self._node_indices:
            from_index: int = self._node_indices[from_node]
            to_index: int = self._node_indices[to_node]
            self._adj_matrix[from_index, to_index] = 1

    def get_adj_matrix(self) -> np.ndarray:
        return self._adj_matrix

    @property
    def num_nodes(self) -> int:
        return self._num_nodes

    @property
    def nodes(self) -> list[dict]:
        return self._nodes
