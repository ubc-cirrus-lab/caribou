import unittest
from caribou.routing.models.instance_indexer import InstanceIndexer


class TestInstanceIndexer(unittest.TestCase):
    def setUp(self):
        self.nodes = [{"instance_name": "node1"}, {"instance_name": "node2"}, {"instance_name": "node3"}]
        self.indexer = InstanceIndexer(self.nodes)

    def test_init(self):
        # Assert
        self.assertEqual(self.indexer._value_indices, {"node1": 0, "node2": 1, "node3": 2})
        self.assertEqual(self.indexer._indices_to_values, {0: "node1", 1: "node2", 2: "node3"})


if __name__ == "__main__":
    unittest.main()
