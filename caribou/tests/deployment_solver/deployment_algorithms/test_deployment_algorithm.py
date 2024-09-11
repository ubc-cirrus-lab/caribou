import unittest
from unittest.mock import patch, MagicMock
from caribou.deployment_solver.deployment_algorithms.deployment_algorithm import DeploymentAlgorithm
from caribou.deployment_solver.workflow_config import WorkflowConfig


class ConcreteDeploymentAlgorithm(DeploymentAlgorithm):
    def __init__(self, workflow_config):
        self._input_manager = MagicMock()
        self._timeout = float("inf")
        pass

    def _run_algorithm(self, timeout: float):
        # Example implementation for testing
        return [(["r1"], {"cost": 100})]


class ConcreteDeploymentAlgorithmCallingSuper(DeploymentAlgorithm):
    def __init__(self, workflow_config):
        super().__init__(workflow_config)

    def _run_algorithm(self):
        # Example implementation for testing
        return [(["r1"], {"cost": 100})]


class TestDeploymentAlgorithm(unittest.TestCase):
    @patch("caribou.deployment_solver.deployment_algorithms.deployment_algorithm.InputManager")
    def setUp(self, mock_input_manager):
        mock_input_manager_instance = MagicMock()
        mock_input_manager.return_value = mock_input_manager_instance
        self.workflow_config_mock = MagicMock(spec=WorkflowConfig)
        self.workflow_config_mock.home_region = "r1:p1"
        self.deployment_algorithm = ConcreteDeploymentAlgorithm(self.workflow_config_mock)

    @patch("caribou.deployment_solver.deployment_algorithms.deployment_algorithm.InputManager")
    @patch("caribou.deployment_solver.deployment_algorithms.deployment_algorithm.RegionIndexer")
    @patch("caribou.deployment_solver.deployment_algorithms.deployment_algorithm.InstanceIndexer")
    @patch("caribou.deployment_solver.deployment_algorithms.deployment_algorithm.SimpleDeploymentMetricsCalculator")
    @patch("caribou.deployment_solver.deployment_algorithms.deployment_algorithm.Ranker")
    @patch("caribou.deployment_solver.deployment_algorithms.deployment_algorithm.Formatter")
    @patch("caribou.deployment_solver.deployment_algorithms.deployment_algorithm.Endpoints")
    def test_init(
        self,
        mock_endpoints,
        mock_formatter,
        mock_ranker,
        mock_simple_deployment_metrics_calculator,
        mock_instance_indexer,
        mock_region_indexer,
        mock_input_manager,
    ):
        # Arrange
        mock_workflow_config = MagicMock()
        mock_workflow_config.home_region = "r1"
        mock_workflow_config.instances.values.return_value = [{"instance_name": "instance1"}]

        # Act
        deployment_algorithm = ConcreteDeploymentAlgorithmCallingSuper(mock_workflow_config)

        # Assert
        self.assertEqual(deployment_algorithm._workflow_config, mock_workflow_config)
        self.assertIsInstance(deployment_algorithm._input_manager, MagicMock)
        self.assertIsInstance(deployment_algorithm._region_indexer, MagicMock)
        self.assertIsInstance(deployment_algorithm._instance_indexer, MagicMock)
        self.assertIsInstance(deployment_algorithm._deployment_metrics_calculator, MagicMock)
        self.assertIsInstance(deployment_algorithm._ranker, MagicMock)
        self.assertIsInstance(deployment_algorithm._formatter, MagicMock)
        self.assertIsInstance(deployment_algorithm._endpoints, MagicMock)

    def test_run(self):
        # Arrange
        self.deployment_algorithm._ranker = MagicMock()
        self.deployment_algorithm._ranker.rank.return_value = [(["r1"], {"cost": 100})]
        self.deployment_algorithm._formatter = MagicMock()
        self.deployment_algorithm._formatter.format.return_value = "formatted_deployment"
        self.deployment_algorithm._region_indexer = MagicMock()
        self.deployment_algorithm._region_indexer.indicies_to_values.return_value = {0: "region1"}
        self.deployment_algorithm._instance_indexer = MagicMock()
        self.deployment_algorithm._instance_indexer.indicies_to_values.return_value = {0: "instance1"}
        self.deployment_algorithm._select_deployment = MagicMock()
        self.deployment_algorithm._select_deployment.return_value = (["r1"], {"cost": 100})
        self.deployment_algorithm._upload_result = MagicMock()
        self.deployment_algorithm._upload_result.return_value = "formatted_deployment"
        self.deployment_algorithm._expiry_time_delta_seconds = 10
        self.deployment_algorithm._number_of_instances = 1
        self.deployment_algorithm._home_region_index = 0
        self.deployment_algorithm._deployment_metrics_calculator = MagicMock()

        # Act
        self.deployment_algorithm.run(["1"])

        # Assert
        self.deployment_algorithm._ranker.rank.assert_called_once()
        self.deployment_algorithm._select_deployment.assert_called_once()
        self.deployment_algorithm._formatter.format.assert_called_once()
        self.deployment_algorithm._upload_result.assert_called_once()

    @patch("caribou.deployment_solver.deployment_algorithms.deployment_algorithm.InputManager.get_all_regions")
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

    @patch("caribou.deployment_solver.deployment_algorithms.deployment_algorithm.Endpoints")
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
        "caribou.deployment_solver.deployment_algorithms.deployment_algorithm.SimpleDeploymentMetricsCalculator.calculate_deployment_metrics"
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

    @patch.object(DeploymentAlgorithm, "_filter_regions_instance")
    def test_get_permitted_region_indices(self, mock_filter_regions_instance):
        # Arrange
        mock_regions = ["r1", "r2", "r3"]
        mock_instance = 0
        mock_filter_regions_instance.return_value = ["r1", "r3"]

        self.deployment_algorithm._region_indexer = MagicMock()
        self.deployment_algorithm._region_indexer.get_value_indices.return_value = {"r1": 0, "r2": 1, "r3": 2}
        self.deployment_algorithm._instance_indexer = MagicMock()

        # Act
        result = self.deployment_algorithm._get_permitted_region_indices(mock_regions, mock_instance)

        # Assert
        self.assertEqual(result, [0, 2])
        mock_filter_regions_instance.assert_called_once_with(mock_regions, mock_instance)
        self.deployment_algorithm._region_indexer.get_value_indices.assert_called_once()


if __name__ == "__main__":
    unittest.main()
