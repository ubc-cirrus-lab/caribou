import unittest
from unittest.mock import Mock, patch
from caribou.deployment_solver.deployment_input.components.loader import InputLoader


class TestInputLoader(unittest.TestCase):
    def setUp(self):
        self.client = Mock()
        self.loader = InputLoader(self.client, "primary_table")

    def test_init(self):
        self.assertEqual(self.loader._client, self.client)
        self.assertEqual(self.loader._primary_table, "primary_table")

    @patch("json.loads")
    def test_retrieve_data(self, mock_json_loads):
        mock_json_loads.return_value = {"key": "value"}
        self.client.get_value_from_table.return_value = ('{"key": "value"}', 0.0)
        result = self.loader._retrieve_data("table", "key")
        self.assertEqual(result, {"key": "value"})

    @patch.object(InputLoader, "_retrieve_data", return_value={"key": "value"})
    def test_retrieve_region_data(self, mock_retrieve_data):
        result = self.loader._retrieve_region_data({"provider1:region1", "provider1:region2"})
        self.assertEqual(result, {"provider1:region1": {"key": "value"}, "provider1:region2": {"key": "value"}})

    def test_str(self):
        self.assertEqual(str(self.loader), "InputLoader(name=InputLoader)")

    def test_repr(self):
        self.assertEqual(repr(self.loader), "InputLoader(name=InputLoader)")


if __name__ == "__main__":
    unittest.main()
