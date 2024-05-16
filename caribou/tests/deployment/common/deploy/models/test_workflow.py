import unittest
from unittest.mock import Mock, MagicMock
from caribou.deployment.common.config.config import Config
from caribou.deployment.common.deploy.models.deployment_package import DeploymentPackage
from caribou.deployment.common.deploy.models.function import Function
from caribou.deployment.common.deploy.models.function_instance import FunctionInstance
from caribou.deployment.common.deploy.models.instructions import Instruction
from caribou.deployment.common.deploy.models.workflow import Workflow
from caribou.deployment_solver.workflow_config import WorkflowConfig

import tempfile
import shutil


class TestWorkflow(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.config = Config({}, self.test_dir)
        self.config.project_config["home_region"] = {"provider": "provider1", "region": "region1"}
        self.function = Mock(spec=Function)
        self.function.deploy = Mock(return_value=True)
        self.function_instance = Mock(spec=FunctionInstance)
        self.function_instance.name = "function_instance_1::"
        self.function_instance.entry_point = True
        self.function_instance.function_resource_name = "function_resource_1"
        self.function_instance.to_json = Mock(return_value={"instance_name": "function_instance_1::"})
        self.function_instance2 = Mock(spec=FunctionInstance)
        self.function_instance2.name = "function_instance_2::"
        self.function_instance2.function_resource_name = "function_resource_2"
        self.function_instance2.to_json = Mock(return_value={"instance_name": "function_instance_2::"})
        self.workflow = Workflow(
            "workflow_name",
            "0.0.1",
            [self.function],
            [self.function_instance, self.function_instance2],
            [("function_instance_1::", "function_instance_2::")],
            self.config,
        )
        self.maxDiff = None

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def test_init(self):
        self.assertEqual(self.workflow.name, "workflow_name")
        self.assertEqual(self.workflow.resource_type, "workflow")
        self.assertEqual(self.workflow._resources, [self.function])
        self.assertEqual(self.workflow._functions, [self.function_instance, self.function_instance2])
        self.assertEqual(self.workflow._edges, [("function_instance_1::", "function_instance_2::")])
        self.assertEqual(self.workflow._config, self.config)

    def test_repr(self):
        self.assertEqual(
            self.workflow.__repr__(),
            f"Workflow(name=workflow_name, resources=[{self.function}], functions=[{self.function_instance}, {self.function_instance2}], edges=[('function_instance_1::', 'function_instance_2::')], config=Config(project_config={{'home_region': {{'provider': 'provider1', 'region': 'region1'}}}}, project_dir={self.test_dir}))",
        )

    def test_get_function_description(self):
        # Mock the to_json method to return a specific value
        function = MagicMock()
        function.to_json.return_value = {"name": "function1"}
        self.workflow._resources = [function]

        # Call the method and check the result
        result = self.workflow.get_function_description()
        self.assertEqual(result, [{"name": "function1"}])

    def test_dependencies(self):
        self.assertEqual(self.workflow.dependencies(), [self.function])

    def test_get_deployment_instructions(self):
        self.function.get_deployment_instructions.return_value = {"region1": [Instruction("test_instruction")]}
        self.assertEqual(self.workflow.get_deployment_instructions(), {"region1": [Instruction("test_instruction")]})

    def test_get_deployment_packages(self):
        self.function.deployment_package = DeploymentPackage("package_name")
        self.assertEqual(self.workflow.get_deployment_packages(), [DeploymentPackage("package_name")])

    def test_get_instance_description(self):
        self.config.project_config["constraints"] = {
            "hard_resource_constraints": {"cost": {"value": 100, "type": "absolute"}},
            "soft_resource_constraints": {"carbon": {"value": 0.1, "type": "relative"}},
            "priority_order": ["cost", "runtime", "carbon"],
        }
        self.config.project_config["regions_and_providers"] = {
            "providers": {
                "provider1": {
                    "config": {"memory": 128, "timeout": 10},
                }
            }
        }
        self.function_instance.to_json.return_value = {
            "function_name": "function_resource_1",
            "instance_name": "function_instance_1::",
        }
        self.function_instance2.to_json.return_value = {
            "function_name": "function_instance_2::",
            "instance_name": "function_instance_2::",
        }
        workflow_config = self.workflow.get_workflow_config()
        self.assertIsInstance(workflow_config, WorkflowConfig)
        self.assertEqual(
            workflow_config._workflow_config["instances"]["function_instance_1::"]["succeeding_instances"],
            ["function_instance_2::"],
        )
        self.assertEqual(
            workflow_config._workflow_config["instances"]["function_instance_1::"]["preceding_instances"], []
        )

    def test_get_instance_description_error_no_config(self):
        self.workflow._config = None
        with self.assertRaises(RuntimeError) as context:
            self.workflow.get_workflow_config()
        self.assertEqual(
            str(context.exception), "Error in workflow config creation, given config is None, this should not happen"
        )

    def test_get_instance_description_error_instances_not_list(self):
        self.function_instance.to_json.return_value = "not a dict"
        with self.assertRaises(RuntimeError) as context:
            self.workflow.get_workflow_config()
        self.assertEqual(str(context.exception), "Error in workflow config creation, this should not happen")

    def test_get_instance_description_error_instance_not_dict(self):
        self.function_instance.to_json.return_value = ["not a dict"]
        with self.assertRaises(RuntimeError) as context:
            self.workflow.get_workflow_config()
        self.assertEqual(str(context.exception), "Error in workflow config creation, this should not happen")

    def test__get_entry_point_instance_name(self):
        self.assertEqual(self.workflow._get_entry_point_instance_name(), "function_instance_1::")

    def test_get_deployed_regions_initial_deployment(self):
        # Mock the _get_function_resource_to_identifier method to return a specific value
        self.workflow._get_function_resource_to_identifier = MagicMock()
        self.workflow._get_function_resource_to_identifier.return_value = {
            "function_resource_1": "identifier1",
            "function_resource_2": "identifier2",
        }

        # Mock the _resources attribute
        mock_function = MagicMock()
        mock_function.name = "function_resource_1"
        mock_function.deploy_region = "region1"

        mock_function2 = MagicMock()
        mock_function2.name = "function_resource_2"
        mock_function2.deploy_region = "region2"

        self.workflow._resources = [mock_function, mock_function2]

        # Define the input
        resource_values = {
            "messaging_topic": [],
            "function": [],
        }

        # Call the method
        result = self.workflow.get_deployed_regions_initial_deployment(resource_values)

        # Define the expected result
        expected_result = {
            "function_resource_1": {
                "deploy_region": "region1",
                "message_topic": "identifier1",
                "function_identifier": "identifier1",
            },
            "function_resource_2": {
                "deploy_region": "region2",
                "message_topic": "identifier2",
                "function_identifier": "identifier2",
            },
        }

        # Assert that the result matches the expected result
        self.assertEqual(result, expected_result)

    def test_update_deployed_regions(self):
        # Mock the _get_function_resource_to_identifier method to return a specific value
        self.workflow._get_function_resource_to_identifier = MagicMock(
            side_effect=lambda x, y: {
                "function_resource_1": "identifier1",
                "function_resource_2": "identifier2",
            }
        )

        # Mock the _resources attribute
        mock_function = MagicMock()
        mock_function.name = "function_resource_1"
        mock_function.deploy_region = "region1"

        mock_function2 = MagicMock()
        mock_function2.name = "function_resource_2"
        mock_function2.deploy_region = "region2"

        self.workflow._resources = [mock_function, mock_function2]

        # Define the input
        resource_values = {
            "messaging_topic": [],
            "function": [],
        }

        deployed_regions = {
            "function_resource_1": {
                "deploy_region": "region1",
                "message_topic": "identifier1",
                "function_identifier": "identifier1",
            },
        }

        # Call the method
        self.workflow.update_deployed_regions(resource_values, deployed_regions)

        # Define the expected result
        expected_result = {
            "function_resource_1": {
                "deploy_region": "region1",
                "message_topic": "identifier1",
                "function_identifier": "identifier1",
            },
            "function_resource_2": {
                "deploy_region": "region2",
                "message_topic": "identifier2",
                "function_identifier": "identifier2",
            },
        }

        # Assert that the _deployed_regions attribute matches the expected result
        self.assertEqual(self.workflow._deployed_regions, expected_result)

    def test_find_all_paths_to_any_sync_node(self):
        self.workflow._edges = [
            ("node1:test:1", "node2:test2:2"),
            ("node2:test2:2", "node3:test3:3"),
            ("node3:test3:3", "node3:sync:4"),
            ("node1:test:1", "node3:sync:4"),
        ]

        expected_paths = [
            ["node1:test:1", "node2:test2:2", "node3:test3:3", "node3:sync:4"],
            ["node1:test:1", "node3:sync:4"],
        ]
        actual_paths = self.workflow._find_all_paths_to_any_sync_node("node1:test:1")
        self.assertEqual(actual_paths, expected_paths)

    def test_find_all_paths_to_any_sync_node_no_sync_node(self):
        self.workflow._edges = [
            ("node1:test:1", "node2:test2:2"),
            ("node2:test2:2", "node3:test3:3"),
            ("node3:test3:3", "node3:sync:4"),
        ]

        expected_paths = [["node1:test:1", "node2:test2:2", "node3:test3:3", "node3:sync:4"]]
        actual_paths = self.workflow._find_all_paths_to_any_sync_node("node1:test:1")
        self.assertEqual(actual_paths, expected_paths)

        expected_paths = [["node2:test2:2", "node3:test3:3", "node3:sync:4"]]
        actual_paths = self.workflow._find_all_paths_to_any_sync_node("node2:test2:2")
        self.assertEqual(actual_paths, expected_paths)

    def test_find_all_paths_to_any_sync_node_no_paths(self):
        self.workflow._edges = [
            ("node1:test:1", "node2:test2:2"),
            ("node2:test2:2", "node3:test3:3"),
            ("node3:test3:3", "node3:sync:4"),
        ]

        expected_paths = []
        actual_paths = self.workflow._find_all_paths_to_any_sync_node("node3:sync:4")
        self.assertEqual(actual_paths, expected_paths)

    def test_find_all_paths_to_any_sync_node_small_sync_example(self):
        self.workflow._edges = [
            ("small_sync_example-0_0_1-initial_function:entry_point:0", "small_sync_example-0_0_1-sync_function:sync:"),
            ("small_sync_example-0_0_1-initial_function:entry_point:0", "small_sync_example-0_0_1-sync_function:sync:"),
            ("small_sync_example-0_0_1-initial_function:entry_point:0", "small_sync_example-0_0_1-sync_function:sync:"),
        ]

        expected_paths = [
            [
                "small_sync_example-0_0_1-initial_function:entry_point:0",
                "small_sync_example-0_0_1-sync_function:sync:",
            ]
        ]

        actual_paths = self.workflow._find_all_paths_to_any_sync_node(
            "small_sync_example-0_0_1-initial_function:entry_point:0"
        )
        self.assertEqual(actual_paths, expected_paths)

    def test_get_workflow_placement(self):
        # Mock the _functions attribute
        mock_function = MagicMock()
        mock_function.name = "function1"
        mock_function.function_resource_name = "resource1"

        self.workflow._functions = [mock_function]

        # Mock the _deployed_regions attribute
        self.workflow._deployed_regions = {
            "resource1": {
                "message_topic": "topic1",
                "function_identifier": "identifier1",
            },
        }

        # Mock the _config object
        self.workflow._config = MagicMock()
        self.workflow._config.home_region = "region1"

        # Call the method
        result = self.workflow._get_workflow_placement()

        # Define the expected result
        expected_result = {
            "function1": {"identifier": "topic1", "provider_region": "region1", "function_identifier": "identifier1"}
        }
        # Assert that the result matches the expected result
        self.assertEqual(result, expected_result)

    def test_get_function_instance_to_resource_name(self):
        # Set up the test data
        staging_area_placement = {
            "function1:instance1": {
                "provider_region": {"provider": "provider1", "region": "region1"},
            },
            "function2:instance2": {
                "provider_region": {"provider": "provider2", "region": "region2"},
            },
        }

        # Call the method
        result = self.workflow._get_function_instance_to_resource_name(staging_area_placement)

        # Define the expected result
        expected_result = {
            "function1:instance1": "function1_provider1-region1",
            "function2:instance2": "function2_provider2-region2",
        }

        # Assert that the result matches the expected result
        self.assertEqual(result, expected_result)

    def test_get_function_resource_to_identifier(self):
        # Define the input
        resource_values = [
            {"name": "function1", "identifier_key": "identifier1"},
            {"name": "function2", "identifier_key": "identifier2"},
        ]
        identifier_key = "identifier_key"

        # Call the method
        result = self.workflow._get_function_resource_to_identifier(resource_values, identifier_key)

        # Define the expected result
        expected_result = {
            "function1": "identifier1",
            "function2": "identifier2",
        }

        # Assert that the result matches the expected result
        self.assertEqual(result, expected_result)

    def test_get_workflow_placement_decision(self):
        # Mock the _get_instances, _get_entry_point_instance_name, and _get_workflow_placement methods
        self.workflow._get_instances = MagicMock(return_value="instances")
        self.workflow._get_entry_point_instance_name = MagicMock(return_value="entry_point_instance_name")
        self.workflow._get_workflow_placement = MagicMock(return_value="workflow_placement")

        # Call the method
        result = self.workflow.get_workflow_placement_decision_initial_deployment()

        # Define the expected result
        expected_result = {
            "instances": "instances",
            "current_instance_name": "entry_point_instance_name",
            "workflow_placement": {"home_deployment": "workflow_placement"},
        }

        # Assert that the result matches the expected result
        self.assertEqual(result, expected_result)

    def test_get_entry_point_from_previous_instances(self):
        # Define the input
        previous_instances = {
            "instance1:entry_point": {"instance_name": "instance1:entry_point"},
            "instance2:other": {"instance_name": "instance2:other"},
        }

        # Call the method
        result = self.workflow._get_entry_point_from_previous_instances(previous_instances)

        # Define the expected result
        expected_result = "instance1:entry_point"

        # Assert that the result matches the expected result
        self.assertEqual(result, expected_result)

    def test_verify_name_and_version(self):
        # Test when version is None
        self.workflow.version = None
        with self.assertRaises(RuntimeError):
            self.workflow.verify_name_and_version()

        # Test when version contains non-numeric and non-dot characters
        self.workflow.version = "1.0a"
        with self.assertRaises(RuntimeError):
            self.workflow.verify_name_and_version()

        # Test when name contains non-alphanumeric and non-underscore characters
        self.workflow.name = "workflow#1"
        with self.assertRaises(RuntimeError):
            self.workflow.verify_name_and_version()

        # Test when name length is more than 25 characters
        self.workflow.name = "a" * 26
        with self.assertRaises(RuntimeError):
            self.workflow.verify_name_and_version()

        # Test when function name contains non-alphanumeric and non-underscore characters
        function = Mock(name="function#1")
        self.workflow._resources = [function]
        with self.assertRaises(RuntimeError):
            self.workflow.verify_name_and_version()

        # Test when function name length is more than 15 characters
        function = Mock(name="a" * 16)
        self.workflow._resources = [function]
        with self.assertRaises(RuntimeError):
            self.workflow.verify_name_and_version()


if __name__ == "__main__":
    unittest.main()
