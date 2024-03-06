import unittest
from unittest.mock import patch, MagicMock
from multi_x_serverless.routing.deployment_algorithms.deployment_algorithm import DeploymentAlgorithm
from multi_x_serverless.routing.workflow_config import WorkflowConfig


class ConcreteDeploymentAlgorithm(DeploymentAlgorithm):
    def __init__(self, workflow_config):
        pass

    def _run_algorithm(self):
        # Example implementation for testing
        return [(["r1"], {"cost": 100})]


class TestDeploymentAlgorithm(unittest.TestCase):
    @patch("multi_x_serverless.routing.deployment_algorithms.deployment_algorithm.InputManager")
    def setUp(self, mock_input_manager):
        mock_input_manager_instance = MagicMock()
        mock_input_manager.return_value = mock_input_manager_instance
        self.workflow_config_mock = MagicMock(spec=WorkflowConfig)
        self.workflow_config_mock.start_hops = "r1:p1"
        self.deployment_algorithm = ConcreteDeploymentAlgorithm(self.workflow_config_mock)

    @patch("multi_x_serverless.routing.deployment_algorithms.deployment_algorithm.InputManager.get_all_regions")
    def test_get_workflow_level_permitted_regions(self, mock_get_all_regions):
        mock_get_all_regions.return_value = ["r1:p1", "r2:p1", "r3:p1"]
        self.workflow_config_mock.regions_and_providers = {
            "providers": {"p1": {}},
            "allowed_regions": ["p1:r1", "p1:r2"],
            "disallowed_regions": None,
        }

        self.deployment_algorithm._input_manager = MagicMock()
        self.deployment_algorithm._input_manager.get_all_regions.return_value = [
            "p1:r1",
            "p1:r2",
            "p1:r3",
        ]

        self.deployment_algorithm._workflow_config = self.workflow_config_mock

        expected_regions = ["p1:r1", "p1:r2"]
        actual_regions = self.deployment_algorithm._get_workflow_level_permitted_regions()

        self.assertEqual(actual_regions, expected_regions)

    def test_filter_regions(self):
        regions = ["p1:r1", "p1:r2", "p2:r1"]
        regions_and_providers = {
            "providers": {"p1": {}, "p2": {}},
            "allowed_regions": None,
            "disallowed_regions": ["p1:r2"],
        }

        expected_regions = ["p1:r1", "p2:r1"]
        actual_regions = self.deployment_algorithm._filter_regions(regions, regions_and_providers)

        self.assertEqual(actual_regions, expected_regions)

    def test_filter_regions_with_allowed_regions(self):
        regions = ["p1:r1", "p1:r2", "p2:r1", "p2:r2"]
        regions_and_providers = {
            "providers": {"p1": {}, "p2": {}},
            "allowed_regions": ["p1:r1", "p2:r1"],
            "disallowed_regions": None,
        }

        expected_regions = ["p1:r1", "p2:r1"]
        actual_regions = self.deployment_algorithm._filter_regions(regions, regions_and_providers)

        self.assertEqual(actual_regions, expected_regions)

    def test_filter_regions_with_disallowed_and_allowed_regions(self):
        regions = ["p1:r1", "p1:r2", "p2:r1", "p2:r2"]
        regions_and_providers = {
            "providers": {"p1": {}, "p2": {}},
            "allowed_regions": ["p1:r1", "p1:r2", "p2:r1"],
            "disallowed_regions": ["p1:r2"],
        }

        expected_regions = ["p1:r1", "p1:r2", "p2:r1"]
        actual_regions = self.deployment_algorithm._filter_regions(regions, regions_and_providers)

        self.assertEqual(actual_regions, expected_regions)

    def test_filter_regions_with_no_allowed_or_disallowed_regions(self):
        regions = ["p1:r1", "p1:r2", "p2:r1", "p2:r2"]
        regions_and_providers = {
            "providers": {"p1": {}, "p2": {}},
            "allowed_regions": None,
            "disallowed_regions": None,
        }

        expected_regions = ["p1:r1", "p1:r2", "p2:r1", "p2:r2"]
        actual_regions = self.deployment_algorithm._filter_regions(regions, regions_and_providers)

        self.assertEqual(actual_regions, expected_regions)

    @patch("multi_x_serverless.routing.deployment_algorithms.deployment_algorithm.Endpoints")
    def test_upload_result(self, mock_endpoints):
        mock_client = MagicMock()
        mock_endpoints.get_deployment_algorithm_workflow_placement_decision_client.return_value = mock_client
        self.deployment_algorithm._endpoints = mock_endpoints
        self.deployment_algorithm._workflow_config = MagicMock()
        self.deployment_algorithm._workflow_config.workflow_id = "workflow_id"

        result = {"deployment": "data"}
        self.deployment_algorithm._upload_result(result)

        mock_client.set_value_in_table.assert_called_once()

    def test_select_deployment(self):
        mock_deployments = [(["r1"], {"cost": 100}), (["r2"], {"cost": 200})]
        selected_deployment = self.deployment_algorithm._select_deployment(mock_deployments)

        self.assertEqual(selected_deployment, mock_deployments[0])

    @patch(
        "multi_x_serverless.routing.deployment_algorithms.deployment_algorithm.SimpleDeploymentMetricsCalculator.calculate_deployment_metrics"
    )
    def test_initialise_home_deployment(self, mock_calculate_deployment_metrics):
        mock_calculate_deployment_metrics.return_value = {"cost": 100}
        self.deployment_algorithm._home_region_index = 0  # Assuming 0 is the index for the home region
        self.deployment_algorithm._number_of_instances = 1

        self.deployment_algorithm._deployment_metrics_calculator = MagicMock()
        self.deployment_algorithm._deployment_metrics_calculator.calculate_deployment_metrics.return_value = {
            "cost": 100
        }

        home_deployment, home_deployment_metrics = self.deployment_algorithm._initialise_home_deployment()

        self.assertEqual(home_deployment, [0])
        self.assertEqual(home_deployment_metrics, {"cost": 100})


if __name__ == "__main__":
    unittest.main()
