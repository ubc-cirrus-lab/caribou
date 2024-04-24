import unittest
from caribou.routing.models.region_indexer import RegionIndexer


class TestRegionIndexer(unittest.TestCase):
    def setUp(self):
        self.regions = ["region1", "region2", "region3"]
        self.indexer = RegionIndexer(self.regions)

    def test_init(self):
        # Assert
        self.assertEqual(self.indexer._value_indices, {"region1": 0, "region2": 1, "region3": 2})
        self.assertEqual(self.indexer._indices_to_values, {0: "region1", 1: "region2", 2: "region3"})


if __name__ == "__main__":
    unittest.main()
