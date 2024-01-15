import unittest
from unittest.mock import Mock
from multi_x_serverless.deployment.common.config.config import Config
from multi_x_serverless.deployment.common.deploy.models.deployment_package import DeploymentPackage
from multi_x_serverless.deployment.common.deploy.models.function import Function
from multi_x_serverless.deployment.common.deploy.models.function_instance import FunctionInstance
from multi_x_serverless.deployment.common.deploy.models.instructions import Instruction
from multi_x_serverless.deployment.common.deploy.models.workflow import Workflow
from multi_x_serverless.routing.current.workflow_config import WorkflowConfig

import tempfile
import shutil


class TestWorkflow(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.config = Config({}, self.test_dir)
        self.function = Mock(spec=Function)
        self.function_instance = Mock(spec=FunctionInstance)
        self.function_instance2 = Mock(spec=FunctionInstance)
        self.workflow = Workflow(
            "workflow_name",
            "0.0.1",
            [self.function],
            [self.function_instance, self.function_instance2],
            [("function_instance_1", "function_instance_2")],
            self.config,
        )

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def test_init(self):
        self.assertEqual(self.workflow.name, "workflow_name")
        self.assertEqual(self.workflow.resource_type, "workflow")
        self.assertEqual(self.workflow._resources, [self.function])
        self.assertEqual(self.workflow._functions, [self.function_instance, self.function_instance2])
        self.assertEqual(self.workflow._edges, [("function_instance_1", "function_instance_2")])
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
        self.config = Mock(spec=Config)
        self.config.home_regions = ["region"]
        self.config.estimated_invocations_per_month = 1000
        self.config.constraints = {"constraint": "value"}
        self.config.regions_and_providers = {"region": "provider"}
        self.function_instance.to_json.return_value = {
            "function_name": "function_instance_1",
            "instance_name": "function_instance_1",
        }
        self.function_instance2.to_json.return_value = {
            "function_name": "function_instance_2",
            "instance_name": "function_instance_2",
        }
        workflow_config = self.workflow.get_instance_description()
        self.assertIsInstance(workflow_config, WorkflowConfig)
        self.assertEqual(
            workflow_config._workflow_config["instances"][0]["succeeding_instances"], ["function_instance_2"]
        )
        self.assertEqual(workflow_config._workflow_config["instances"][0]["preceding_instances"], [])

    def test_get_instance_description_error_no_config(self):
        self.workflow._config = None
        with self.assertRaises(RuntimeError) as context:
            self.workflow.get_instance_description()
        self.assertEqual(
            str(context.exception), "Error in workflow config creation, given config is None, this should not happen"
        )

    def test_get_instance_description_error_instances_not_list(self):
        self.function_instance.to_json.return_value = "not a dict"
        with self.assertRaises(RuntimeError) as context:
            self.workflow.get_instance_description()
        self.assertEqual(str(context.exception), "Error in workflow config creation, this should not happen")

    def test_get_instance_description_error_instance_not_dict(self):
        self.function_instance.to_json.return_value = ["not a dict"]
        with self.assertRaises(RuntimeError) as context:
            self.workflow.get_instance_description()
        self.assertEqual(str(context.exception), "Error in workflow config creation, this should not happen")


if __name__ == "__main__":
    unittest.main()
