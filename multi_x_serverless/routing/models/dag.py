from collections import deque

import numpy as np

from multi_x_serverless.routing.models.indexer import Indexer


class DAG(Indexer):
    def __init__(self, nodes: list[dict]) -> None:
        self._nodes: list[dict] = nodes
        self._num_nodes: int = len(nodes)
        self._adj_matrix: np.ndarray = np.zeros((self.num_nodes, self.num_nodes), dtype=int)

        self._value_indices: dict[str, int] = {node["instance_name"]: index for index, node in enumerate(nodes)}

    def add_edge(self, from_node: str, to_node: str) -> None:
        if from_node in self._value_indices and to_node in self._value_indices:
            from_index: int = self._value_indices[from_node]
            to_index: int = self._value_indices[to_node]
            self._adj_matrix[from_index, to_index] = 1

    def topological_sort(self) -> list[int]:
        in_degree = np.sum(self._adj_matrix, axis=0)
        queue: deque[int] = deque()

        for i, node in enumerate(self._nodes):
            if in_degree[i] == 0:
                queue.append(i)

        result = []
        while queue:
            node_index = queue.popleft()

            instance_name = self._nodes[node_index]["instance_name"]
            result.append(self._value_indices[instance_name])

            for i in range(self.num_nodes):
                if self._adj_matrix[node_index, i] == 1:
                    in_degree[i] -= 1
                    if in_degree[i] == 0:
                        queue.append(i)

        # Check for cycles
        if len(result) != self.num_nodes:
            raise ValueError("The graph contains a cycle")

        return result

    def get_preceeding_dict(self) -> dict[int, list[int]]:
        # Initialize an empty prerequisites dictionary
        preceeding: dict[int, list[int]] = {}

        # Iterate through each row in the adjacency matrix to find prerequisites for each node
        for i, row in enumerate(self._adj_matrix):
            preceeding[i] = []

            # Find nodes that have incoming edges to the current node (prerequisites)
            for j, val in enumerate(row):
                if val == 1:
                    preceeding[i].append(j)

        return preceeding

    def get_prerequisites_dict(self) -> dict[int, list[int]]:
        # Initialize an empty prerequisites dictionary
        prerequisites: dict[int, list[int]] = {}

        # Iterate through each row in the adjacency matrix to find prerequisites for each node
        for i, row in enumerate(self._adj_matrix):
            prerequisites[i] = []

        # Find nodes that have incoming edges to the current node (prerequisites)
        for i, row in enumerate(self._adj_matrix):
            for j, val in enumerate(row):
                if val == 1:
                    prerequisites[j].append(i)

        return prerequisites

    def get_leaf_nodes(self) -> list[int]:
        in_degree = np.sum(self._adj_matrix, axis=1)
        leaf_nodes = []

        for i in range(self.num_nodes):
            if in_degree[i] == 0:
                instance_name = self._nodes[i]["instance_name"]
                leaf_nodes.append(self._value_indices[instance_name])

        return leaf_nodes

    def get_adj_matrix(self) -> np.ndarray:
        return self._adj_matrix

    def values_to_indices(self, instances: np.ndarray) -> np.ndarray:
        return np.array([self._value_indices[instance] for instance in instances])

    def indicies_to_values(self, indices: np.ndarray) -> np.ndarray:
        # Can be optimized
        reverse_mapping = {index: instance for instance, index in self._value_indices.items()}
        return np.array([reverse_mapping.get(index) for index in indices])

    @property
    def num_nodes(self) -> int:
        return self._num_nodes

    @property
    def nodes(self) -> list[dict]:
        return self._nodes

    @property
    def number_of_edges(self) -> int:
        return np.sum(self._adj_matrix)
