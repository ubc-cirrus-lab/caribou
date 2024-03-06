import unittest
from unittest.mock import patch, MagicMock
from multi_x_serverless.routing.deployment_algorithms.deployment_algorithm import DeploymentAlgorithm
from multi_x_serverless.routing.workflow_config import WorkflowConfig


class ConcreteDeploymentAlgorithm(DeploymentAlgorithm):
    def _run_algorithm(self):
        # Example implementation for testing
        return [(["us-east-1"], {"cost": 100})]


class TestDeploymentAlgorithm(unittest.TestCase):
    @patch("multi_x_serverless.routing.deployment_algorithms.deployment_algorithm.InputManager")
    def setUp(self, mock_input_manager):
        mock_input_manager_instance = MagicMock()
        mock_input_manager.return_value = mock_input_manager_instance
        self.workflow_config_mock = MagicMock(spec=WorkflowConfig)
        self.workflow_config_mock.start_hops = "us-east-1:aws"
        self.deployment_algorithm = ConcreteDeploymentAlgorithm(self.workflow_config_mock)

    @patch("multi_x_serverless.routing.deployment_algorithms.deployment_algorithm.InputManager.get_all_regions")
    def test_get_workflow_level_permitted_regions(self, mock_get_all_regions):
        mock_get_all_regions.return_value = ["us-east-1:aws", "eu-west-1:aws", "ap-southeast-1:aws"]
        self.workflow_config_mock.regions_and_providers = {
            "providers": {"aws": {}},
            "allowed_regions": ["us-east-1:aws", "eu-west-1:aws"],
            "disallowed_regions": None,
        }

        expected_regions = ["us-east-1:aws", "eu-west-1:aws"]
        actual_regions = self.deployment_algorithm._get_workflow_level_permitted_regions()

        self.assertEqual(actual_regions, expected_regions)

    def test_filter_regions(self):
        regions = ["us-east-1:aws", "eu-west-1:azure", "ap-southeast-1:aws"]
        regions_and_providers = {
            "providers": {"aws": {}},
            "allowed_regions": None,
            "disallowed_regions": ["eu-west-1:azure"],
        }

        expected_regions = ["us-east-1:aws", "ap-southeast-1:aws"]
        actual_regions = self.deployment_algorithm._filter_regions(regions, regions_and_providers)

        self.assertEqual(actual_regions, expected_regions)

    @patch("multi_x_serverless.routing.deployment_algorithms.deployment_algorithm.Endpoints")
    def test_upload_result(self, mock_endpoints):
        mock_client = MagicMock()
        mock_endpoints.get_deployment_algorithm_workflow_placement_decision_client.return_value = mock_client
        self.deployment_algorithm._endpoints = mock_endpoints

        result = {"deployment": "data"}
        self.deployment_algorithm._upload_result(result)

        mock_client.set_value_in_table.assert_called_once()

    def test_select_deployment(self):
        mock_deployments = [(["us-east-1"], {"cost": 100}), (["eu-west-1"], {"cost": 200})]
        selected_deployment = self.deployment_algorithm._select_deployment(mock_deployments)

        self.assertEqual(selected_deployment, mock_deployments[0])

    @patch(
        "multi_x_serverless.routing.deployment_algorithms.deployment_algorithm.SimpleDeploymentMetricsCalculator.calculate_deployment_metrics"
    )
    def test_initialise_home_deployment(self, mock_calculate_deployment_metrics):
        mock_calculate_deployment_metrics.return_value = {"cost": 100}
        self.deployment_algorithm._home_region_index = 0  # Assuming 0 is the index for the home region
        self.deployment_algorithm._number_of_instances = 1

        home_deployment, home_deployment_metrics = self.deployment_algorithm._initialise_home_deployment()

        self.assertEqual(home_deployment, [0])
        self.assertEqual(home_deployment_metrics, {"cost": 100})


if __name__ == "__main__":
    unittest.main()
