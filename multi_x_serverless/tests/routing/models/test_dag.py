import unittest
import numpy as np

from multi_x_serverless.routing.current.models.dag import DAG

# TODO
class TestDAG(unittest.TestCase):
    def setUp(self):
        self.nodes = [{"instance_name": "node1"}, {"instance_name": "node2"}, {"instance_name": "node3"}]
        self.dag = DAG(self.nodes)

    def test_init(self):
        self.assertEqual(self.dag.nodes, self.nodes)
        self.assertEqual(self.dag.num_nodes, len(self.nodes))
        np.testing.assert_array_equal(self.dag.get_adj_matrix(), np.zeros((len(self.nodes), len(self.nodes))))

    def test_add_edge(self):
        self.dag.add_edge("node1", "node2")
        expected_matrix = np.zeros((len(self.nodes), len(self.nodes)))
        expected_matrix[0, 1] = 1
        np.testing.assert_array_equal(self.dag.get_adj_matrix(), expected_matrix)

    def test_add_edge_nonexistent_nodes(self):
        self.dag.add_edge("node4", "node5")
        expected_matrix = np.zeros((len(self.nodes), len(self.nodes)))
        np.testing.assert_array_equal(self.dag.get_adj_matrix(), expected_matrix)


if __name__ == "__main__":
    unittest.main()
