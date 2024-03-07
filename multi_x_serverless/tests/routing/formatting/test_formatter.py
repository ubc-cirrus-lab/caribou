import unittest
from datetime import datetime, timedelta
from multi_x_serverless.routing.formatter.formatter import Formatter
from unittest.mock import patch


class TestFormatter(unittest.TestCase):
    @patch("multi_x_serverless.routing.formatter.formatter.timedelta")
    @patch("multi_x_serverless.routing.formatter.formatter.datetime")
    def test_format(self, mock_datetime, mock_timedelta):
        self.maxDiff = None
        # Arrange
        home_deployment = [0, 1]
        home_deployment_metrics = {"average_carbon": 0.1, "average_runtime": 0.2, "average_cost": 0.3}
        formatter = Formatter(home_deployment, home_deployment_metrics)
        results = ([0, 1], {"average_carbon": 0.2, "average_runtime": 0.3, "average_cost": 0.4})
        index_to_instance_name = {0: "instance1", 1: "instance2", 2: "instance3", 3: "instance4"}
        index_to_region_provider_name = {
            0: "AWS:us-east-1",
            1: "AWS:us-west-2",
        }
        mock_datetime.now.return_value = datetime(2021, 1, 1, 0, 0, 0)
        mock_timedelta.return_value = timedelta(seconds=604800)

        # Act
        formatted_results = formatter.format(results, index_to_instance_name, index_to_region_provider_name)

        # Assert
        self.assertEqual(
            formatted_results,
            {
                "workflow_placement": {
                    "current_deployment": {
                        "instances": {
                            "instance1": {"provider_region": {"provider": "AWS", "region": "us-east-1"}},
                            "instance2": {"provider_region": {"provider": "AWS", "region": "us-west-2"}},
                        },
                        "metrics": {"average_carbon": 0.2, "average_runtime": 0.3, "average_cost": 0.4},
                        "expiry_time": "2021-01-08 00:00:00",
                    },
                    "home_deployment": {
                        "instances": {
                            "instance1": {"provider_region": {"provider": "AWS", "region": "us-east-1"}},
                            "instance2": {"provider_region": {"provider": "AWS", "region": "us-west-2"}},
                        },
                        "metrics": {"average_carbon": 0.1, "average_runtime": 0.2, "average_cost": 0.3},
                    },
                }
            },
        )


if __name__ == "__main__":
    unittest.main()
