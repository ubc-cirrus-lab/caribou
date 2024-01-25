import unittest
from unittest.mock import Mock
from multi_x_serverless.deployment.common.config.config import Config
from multi_x_serverless.deployment.common.deploy.models.deployment_package import DeploymentPackage
from multi_x_serverless.deployment.common.deploy.models.function import Function
from multi_x_serverless.deployment.common.deploy.models.function_instance import FunctionInstance
from multi_x_serverless.deployment.common.deploy.models.instructions import Instruction
from multi_x_serverless.deployment.common.deploy.models.workflow import Workflow
from multi_x_serverless.routing.workflow_config import WorkflowConfig

import tempfile
import shutil


class TestWorkflow(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.config = Config({}, self.test_dir)
        self.config.project_config["home_regions"] = [{"provider": "aws", "region": "region"}]
        self.function = Mock(spec=Function)
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

    def test_dependencies(self):
        self.assertEqual(self.workflow.dependencies(), [self.function])

    def test_get_deployment_instructions(self):
        self.function.get_deployment_instructions.return_value = {"region": [Instruction("test_instruction")]}
        self.assertEqual(self.workflow.get_deployment_instructions(), {"region": [Instruction("test_instruction")]})

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
                "aws": {
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
            workflow_config._workflow_config["instances"][0]["succeeding_instances"], ["function_instance_2::"]
        )
        self.assertEqual(workflow_config._workflow_config["instances"][0]["preceding_instances"], [])

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

    def test_get_workflow_placement(self):
        resource_values = {
            "sns_topic": [
                {"name": "function_resource_1", "topic_identifier": "identifier_1"},
                {"name": "function_resource_2", "topic_identifier": "identifier_2"},
            ]
        }
        expected_output = {
            "function_instance_1::": {
                "identifier": "identifier_1",
                "provider_region": {"provider": "aws", "region": "region"},
            },
            "function_instance_2::": {
                "identifier": "identifier_2",
                "provider_region": {"provider": "aws", "region": "region"},
            },
        }
        self.assertEqual(self.workflow._get_workflow_placement(resource_values), expected_output)

    def test_extend_stage_area_placement(self):
        resource_values = {
            "sns_topic": [
                {"name": "function_resource_1", "topic_identifier": "identifier_1"},
                {"name": "function_resource_2", "topic_identifier": "identifier_2"},
            ]
        }
        staging_area_placement = {"workflow_placement": {"function_instance_1::": {}, "function_instance_2::": {}}}
        expected_output = {
            "workflow_placement": {
                "function_instance_1::": {"identifier": "identifier_1"},
                "function_instance_2::": {"identifier": "identifier_2"},
            }
        }
        self.assertEqual(
            self.workflow._extend_stage_area_placement(resource_values, staging_area_placement),
            expected_output,
        )

    def test_get_function_instance_to_identifier(self):
        resource_values = {
            "sns_topic": [
                {"name": "function_resource_1", "topic_identifier": "identifier_1"},
                {"name": "function_resource_2", "topic_identifier": "identifier_2"},
            ]
        }
        expected_output = {"function_instance_1::": "identifier_1", "function_instance_2::": "identifier_2"}
        self.assertEqual(self.workflow._get_function_instance_to_identifier(resource_values), expected_output)

    def test_get_workflow_placement_decision(self):
        resource_values = {
            "sns_topic": [
                {"name": "function_resource_1", "topic_identifier": "identifier_1"},
                {"name": "function_resource_2", "topic_identifier": "identifier_2"},
            ]
        }
        self.workflow.get_workflow_config = Mock(return_value=Mock(instances=["instance_1", "instance_2"]))
        self.workflow._Workflow__get_entry_point_instance_name = Mock(return_value="entry_point_instance")
        expected_output = {
            "instances": [
                {
                    "instance_name": "function_instance_1::",
                    "preceding_instances": [],
                    "regions_and_providers": {},
                    "succeeding_instances": ["function_instance_2::"],
                    "dependent_sync_predecessors": [],
                },
                {
                    "instance_name": "function_instance_2::",
                    "preceding_instances": ["function_instance_1::"],
                    "regions_and_providers": {},
                    "succeeding_instances": [],
                    "dependent_sync_predecessors": [],
                },
            ],
            "current_instance_name": "function_instance_1::",
            "workflow_placement": {
                "function_instance_1::": {
                    "identifier": "identifier_1",
                    "provider_region": {"provider": "aws", "region": "region"},
                },
                "function_instance_2::": {
                    "identifier": "identifier_2",
                    "provider_region": {"provider": "aws", "region": "region"},
                },
            },
        }
        self.assertEqual(self.workflow.get_workflow_placement_decision(resource_values), expected_output)

    def test_get_workflow_placement_decision_extend_staging(self):
        resource_values = {
            "sns_topic": [
                {"name": "function_resource_1", "topic_identifier": "identifier_1"},
                {"name": "function_resource_2", "topic_identifier": "identifier_2"},
            ]
        }
        staging_area_placement = {
            "workflow_placement": {
                "function_instance_1::": {},
                "function_instance_2::": {},
            }
        }
        self.workflow.get_workflow_config = Mock(return_value=Mock(instances=["instance_1", "instance_2"]))
        self.workflow._Workflow__get_entry_point_instance_name = Mock(return_value="entry_point_instance")
        expected_output = {
            "instances": [
                {
                    "instance_name": "function_instance_1::",
                    "preceding_instances": [],
                    "regions_and_providers": {},
                    "succeeding_instances": ["function_instance_2::"],
                    "dependent_sync_predecessors": [],
                },
                {
                    "instance_name": "function_instance_2::",
                    "preceding_instances": ["function_instance_1::"],
                    "regions_and_providers": {},
                    "succeeding_instances": [],
                    "dependent_sync_predecessors": [],
                },
            ],
            "current_instance_name": "function_instance_1::",
            "workflow_placement": {
                "function_instance_1::": {"identifier": "identifier_1"},
                "function_instance_2::": {"identifier": "identifier_2"},
            },
        }
        self.assertEqual(
            self.workflow.get_workflow_placement_decision_extend_staging(resource_values, staging_area_placement),
            expected_output,
        )

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


if __name__ == "__main__":
    unittest.main()
