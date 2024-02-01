import unittest
import numpy as np

from multi_x_serverless.routing.models.dag import DAG
import time


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

    def test_topological_sort(self):
        self.dag.add_edge("node1", "node2")
        self.dag.add_edge("node2", "node3")
        self.dag.add_edge("node1", "node3")

        self.assertEqual(self.dag.topological_sort(), [0, 1, 2])

    def test_topological_sort_complex_1(self):
        dag = DAG([{"instance_name": "node1"}, {"instance_name": "node2"}, {"instance_name": "node3"}, {"instance_name": "node4"}, {"instance_name": "node5"}])
        dag.add_edge("node1", "node2")
        dag.add_edge("node2", "node3")
        dag.add_edge("node3", "node4")
        dag.add_edge("node4", "node5")

        self.assertEqual(dag.topological_sort(), [0, 1, 2, 3, 4])

    def test_topological_sort_complex_2(self):
        dag = DAG([{"instance_name": "node1"}, {"instance_name": "node2"}, {"instance_name": "node3"}, {"instance_name": "node4"}])
        dag.add_edge("node1", "node2")
        dag.add_edge("node1", "node3")
        dag.add_edge("node2", "node4")
        dag.add_edge("node3", "node4")

        self.assertEqual(dag.topological_sort(), [0, 1, 2, 3])

    def test_topological_sort_complex_3(self):
        self.dag.add_edge("node1", "node2")
        self.dag.add_edge("node2", "node3")
        self.dag.add_edge("node3", "node1")

        with self.assertRaises(Exception):
            self.dag.topological_sort()

    def test_topological_sort_complex_4(self):
        dag = DAG([{"instance_name": "node1"}, {"instance_name": "node2"}, {"instance_name": "node3"}, {"instance_name": "node4"}])
        dag.add_edge("node1", "node2")
        dag.add_edge("node2", "node3")
        dag.add_edge("node3", "node4")
        dag.add_edge("node4", "node1")

        with self.assertRaises(Exception):
            dag.topological_sort()

    def test_topological_sort_complex_5(self):
        dag = DAG([{"instance_name": "node1"}, {"instance_name": "node2"}, {"instance_name": "node3"}, {"instance_name": "node4"}, {"instance_name": "node5"}, {"instance_name": "node6"}])
        dag.add_edge("node1", "node2")
        dag.add_edge("node2", "node3")
        dag.add_edge("node3", "node4")
        dag.add_edge("node4", "node5")
        dag.add_edge("node5", "node6")

        self.assertEqual(dag.topological_sort(), [0, 1, 2, 3, 4, 5])
    
    def test_topological_sort_complex_large(self):
        nodes = [{"instance_name": f"node{i}"} for i in range(1, 1001)]
        dag = DAG(nodes)

        for i in range(1, 1000):
            dag.add_edge(f"node{i}", f"node{i+1}")

        self.assertEqual(dag.topological_sort(), list(range(1000)))

    def test_topological_sort_cycle(self):
        self.dag.add_edge("node1", "node2")
        self.dag.add_edge("node2", "node3")
        self.dag.add_edge("node3", "node1")

        with self.assertRaises(ValueError):
            self.dag.topological_sort()


if __name__ == "__main__":
    unittest.main()
