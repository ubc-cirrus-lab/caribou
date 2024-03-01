import unittest
from multi_x_serverless.routing.formatter.formatter import Formatter


class TestFormatter(unittest.TestCase):
    def setUp(self):
        self.formatter = Formatter(
            [0, 0],
            {
                "average_cost": 0.0,
                "average_runtime": 0.0,
                "average_carbon": 0.0,
                "tail_cost": 0.0,
                "tail_runtime": 0.0,
                "tail_carbon": 0.0,
            },
        )

    def test_format(self):
        results = (
            {0: 1, 1: 0},
            {
                "average_cost": 0.0,
                "average_runtime": 0.0,
                "average_carbon": 0.0,
                "tail_cost": 0.0,
                "tail_runtime": 0.0,
                "tail_carbon": 0.0,
            },
        )
        index_to_instance_name = {0: "instance1", 1: "instance2"}
        index_to_region_provider_name = {0: "provider1:region1", 1: "provider2:region2"}

        expected_output = {
            "current_deployment": {
                "workflow_placement": {
                    "instance1": {"provider_region": {"provider": "provider1", "region": "region1"}},
                    "instance2": {"provider_region": {"provider": "provider2", "region": "region2"}},
                },
                "metrics": {
                    "average_cost": 0.0,
                    "average_runtime": 0.0,
                    "average_carbon": 0.0,
                    "tail_cost": 0.0,
                    "tail_runtime": 0.0,
                    "tail_carbon": 0.0,
                },
            },
            "home_deployment": {
                "workflow_placement": {
                    "instance1": {"provider_region": {"provider": "provider1", "region": "region1"}},
                    "instance2": {"provider_region": {"provider": "provider1", "region": "region1"}},
                },
                "metrics": {
                    "average_cost": 0.0,
                    "average_runtime": 0.0,
                    "average_carbon": 0.0,
                    "tail_cost": 0.0,
                    "tail_runtime": 0.0,
                    "tail_carbon": 0.0,
                },
            },
        }

        output = self.formatter.format(results, index_to_instance_name, index_to_region_provider_name)

        print(output)
        self.assertEqual(output, expected_output)


if __name__ == "__main__":
    unittest.main()
