import unittest
from multi_x_serverless.routing.formatter.formatter import Formatter


class TestFormatter(unittest.TestCase):
    def setUp(self):
        self.formatter = Formatter()
        self.maxDiff = None

    def test_format(self):
        results = ({0: 1, 1: 0}, 0.0, 0.0, 0.0)
        index_to_instance_name = {0: "instance1", 1: "instance2"}
        index_to_region_provider_name = {0: "provider1:region1", 1: "provider2:region2"}

        expected_output = {
            "workflow_placement": {
                "current_deployment": {
                    "instances": {
                        "instance1": {"provider_region": {"provider": "provider2", "region": "region2"}},
                        "instance2": {"provider_region": {"provider": "provider1", "region": "region1"}},
                    }
                },
            },
        }

        output = self.formatter.format(results, index_to_instance_name, index_to_region_provider_name)

        self.assertEqual(output, expected_output)


if __name__ == "__main__":
    unittest.main()
