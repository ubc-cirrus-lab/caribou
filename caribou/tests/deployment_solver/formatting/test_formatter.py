import unittest
from datetime import datetime, timedelta
from caribou.deployment_solver.formatter.formatter import Formatter
from unittest.mock import patch


class TestFormatter(unittest.TestCase):
    def test_format(self):
        self.maxDiff = None
        # Arrange
        formatter = Formatter()
        results = ([0, 1], {"average_carbon": 0.2, "average_runtime": 0.3, "average_cost": 0.4})
        index_to_instance_name = {0: "instance1", 1: "instance2", 2: "instance3", 3: "instance4"}
        index_to_region_provider_name = {
            0: "AWS:us-east-1",
            1: "AWS:us-west-2",
        }

        # Act
        formatted_results = formatter.format(results, index_to_instance_name, index_to_region_provider_name)

        # Assert
        self.assertEqual(
            formatted_results,
            {
                "instance1": {"provider_region": {"provider": "AWS", "region": "us-east-1"}},
                "instance2": {"provider_region": {"provider": "AWS", "region": "us-west-2"}},
            },
        )


if __name__ == "__main__":
    unittest.main()
