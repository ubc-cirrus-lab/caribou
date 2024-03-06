from collections import deque

import numpy as np

from multi_x_serverless.routing.models.instance_indexer import InstanceIndexer


class DAG:
    def __init__(self, workflow_config_instances: list[dict], instance_indexer: InstanceIndexer) -> None:
        super().__init__()
        self._nodes: list[dict] = [
            {k: v for k, v in node.items() if k not in ("succeeding_instances", "preceding_instances")}
            for node in workflow_config_instances
        ]

        self._num_nodes: int = len(self._nodes)
        self._adj_matrix: np.ndarray = np.zeros((self.num_nodes, self.num_nodes), dtype=int)

        self._value_indices: dict[str, int] = instance_indexer.get_value_indices()

        for instance in workflow_config_instances:
            for succeeding_instance in instance["succeeding_instances"]:
                self._add_edge(instance["instance_name"], succeeding_instance)

    def _add_edge(self, from_node: str, to_node: str) -> None:
        if from_node in self._value_indices and to_node in self._value_indices:
            from_index: int = self._value_indices[from_node]
            to_index: int = self._value_indices[to_node]
            self._adj_matrix[from_index, to_index] = 1

    def topological_sort(self) -> list[int]:
        in_degree = np.sum(self._adj_matrix, axis=0)
        queue = deque([i for i in range(self._num_nodes) if in_degree[i] == 0])

        result = []
        while queue:
            node_index = queue.popleft()
            result.append(node_index)

            for i in range(self._num_nodes):
                if self._adj_matrix[node_index, i] == 1:
                    in_degree[i] -= 1
                    if in_degree[i] == 0:
                        queue.append(i)

        # Check for cycles
        if len(result) != self.num_nodes:
            raise ValueError("The graph contains a cycle")

        return result

    def get_preceeding_dict(self) -> dict[int, list[int]]:
        # Initialize an empty successors dictionary
        preceeding: dict[int, list[int]] = {}

        # Iterate through each row in the adjacency matrix to find successors for each node
        for i, row in enumerate(self._adj_matrix):
            preceeding[i] = []

            # Find nodes that is the outgoing edge of the current node (successors)
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

    @property
    def num_nodes(self) -> int:
        return self._num_nodes

    @property
    def nodes(self) -> list[dict]:
        return self._nodes

    @property
    def number_of_edges(self) -> int:
        return np.sum(self._adj_matrix)
