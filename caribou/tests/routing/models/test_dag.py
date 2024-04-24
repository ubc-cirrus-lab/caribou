import unittest
from unittest.mock import MagicMock, patch
from caribou.routing.models.dag import DAG
from caribou.routing.models.instance_indexer import InstanceIndexer
import numpy as np


class TestDAG(unittest.TestCase):
    @patch.object(DAG, "__init__", return_value=None)
    def setUp(self, mock_init):
        self.dag = DAG()
        self.dag._value_indices = {"node1": 0, "node2": 1}
        self.dag._adj_matrix = np.zeros((2, 2), dtype=int)

    def test_add_edge(self):
        # Act
        self.dag._add_edge("node1", "node2")

        # Assert
        self.assertEqual(self.dag._adj_matrix[0, 1], 1)

    @patch.object(DAG, "_add_edge")
    def test_init(self, mock_add_edge):
        # Arrange
        workflow_config_instances = [
            {
                "instance_name": "instance1",
                "succeeding_instances": ["instance2"],
                "preceding_instances": [],
            },
            {
                "instance_name": "instance2",
                "succeeding_instances": [],
                "preceding_instances": ["instance1"],
            },
        ]
        mock_instance_indexer = MagicMock(spec=InstanceIndexer)
        mock_instance_indexer.get_value_indices.return_value = {"instance1": 0, "instance2": 1}

        # Act
        dag = DAG(workflow_config_instances, mock_instance_indexer)

        # Assert
        self.assertEqual(dag._nodes, [{"instance_name": "instance1"}, {"instance_name": "instance2"}])
        self.assertEqual(dag._num_nodes, 2)
        self.assertEqual(dag._value_indices, {"instance1": 0, "instance2": 1})
        mock_add_edge.assert_called_once_with("instance1", "instance2")

    def test_topological_sort(self):
        # Arrange
        self.dag._num_nodes = 3
        self.dag._adj_matrix = np.array([[0, 1, 0], [0, 0, 1], [0, 0, 0]])

        # Act
        result = self.dag.topological_sort()

        # Assert
        self.assertEqual(result, [0, 1, 2])

    def test_topological_sort_with_cycle(self):
        # Arrange
        self.dag._num_nodes = 3
        self.dag._adj_matrix = np.array([[0, 1, 0], [0, 0, 1], [1, 0, 0]])

        # Act & Assert
        with self.assertRaises(ValueError):
            self.dag.topological_sort()

    def test_get_preceeding_dict(self):
        # Arrange
        self.dag._adj_matrix = np.array([[0, 1, 0], [0, 0, 1], [1, 0, 0]])

        # Act
        result = self.dag.get_preceeding_dict()

        # Assert
        self.assertEqual(result, {0: [1], 1: [2], 2: [0]})

    def test_get_leaf_nodes(self):
        # Arrange
        self.dag._adj_matrix = np.array([[0, 1, 0], [0, 0, 1], [0, 0, 0]])
        self.dag._num_nodes = 3
        self.dag._nodes = [{"instance_name": "node1"}, {"instance_name": "node2"}, {"instance_name": "node3"}]
        self.dag._value_indices = {"node1": 0, "node2": 1, "node3": 2}

        # Act
        result = self.dag.get_leaf_nodes()

        # Assert
        self.assertEqual(result, [2])

    def test_get_adj_matrix(self):
        # Act
        result = self.dag.get_adj_matrix()

        # Assert
        np.testing.assert_array_equal(result, self.dag._adj_matrix)

    def test_num_nodes(self):
        # Arrange
        self.dag._num_nodes = 3

        # Act
        result = self.dag.num_nodes

        # Assert
        self.assertEqual(result, self.dag._num_nodes)

    def test_nodes(self):
        # Arrange
        self.dag._nodes = [1, 2, 3]

        # Act
        result = self.dag.nodes

        # Assert
        self.assertEqual(result, self.dag._nodes)

    def test_number_of_edges(self):
        # Arrange
        self.dag._adj_matrix = np.array([[0, 1, 0], [0, 0, 1], [1, 0, 0]])

        # Act
        result = self.dag.number_of_edges

        # Assert
        self.assertEqual(result, 3)

    def test_get_prerequisites_dict(self):
        # Arrange
        self.dag._adj_matrix = np.array([[0, 1, 0], [0, 0, 1], [1, 0, 0]])

        # Act
        result = self.dag.get_prerequisites_dict()

        # Assert
        self.assertEqual(result, {0: [2], 1: [0], 2: [1]})


if __name__ == "__main__":
    unittest.main()
