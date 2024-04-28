import unittest
from caribou.deployment_solver.models.indexer import Indexer


class TestIndexer(unittest.TestCase):
    class SubIndexer(Indexer):
        def __init__(self):
            self._value_indices = {"value1": 0, "value2": 1}
            super().__init__()

    class SubIndexerWithoutValueIndices(Indexer):
        def __init__(self):
            super().__init__()

    def setUp(self):
        self.indexer = self.SubIndexer()

    def test_init(self):
        indexer = self.SubIndexerWithoutValueIndices()
        # Assert
        self.assertEqual(indexer._value_indices, {})

    def test_get_value_indices(self):
        # Act
        result = self.indexer.get_value_indices()

        # Assert
        self.assertEqual(result, {"value1": 0, "value2": 1})

    def test_indicies_to_values(self):
        # Act
        result = self.indexer.indicies_to_values()

        # Assert
        self.assertEqual(result, {0: "value1", 1: "value2"})

    def test_value_to_index(self):
        # Act
        result = self.indexer.value_to_index("value1")

        # Assert
        self.assertEqual(result, 0)

    def test_index_to_value(self):
        # Act
        result = self.indexer.index_to_value(1)

        # Assert
        self.assertEqual(result, "value2")


if __name__ == "__main__":
    unittest.main()
