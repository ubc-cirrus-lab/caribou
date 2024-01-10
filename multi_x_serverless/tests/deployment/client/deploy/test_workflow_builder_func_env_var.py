import os
import unittest
from unittest.mock import Mock, patch
from multi_x_serverless.deployment.client.config import Config
from multi_x_serverless.deployment.client.multi_x_serverless_workflow import MultiXServerlessFunction
from multi_x_serverless.deployment.client.deploy.workflow_builder import WorkflowBuilder


class TestWorkflowBuilderFuncEnvVar(unittest.TestCase):
    def test_build_func_environment_variables(self):
        # function 1 (empty function level environment variables)
        function1 = Mock(spec=MultiXServerlessFunction)
        function1.entry_point = True
        function1.name = "function1"
        function1.handler = "function1"
        function1.regions_and_providers = {}
        function1.func_environment_variables = []

        # function 2 (no overlap with global environment variables)
        function2 = Mock(spec=MultiXServerlessFunction)
        function2.entry_point = False
        function2.name = "function2"
        function2.handler = "function1"
        function2.regions_and_providers = {"providers": []}
        function2.func_environment_variables = [{"key": "ENV_3", "value": "function2_env_3"}]

        # function 3 (overlap with global environment variables)
        function3 = Mock(spec=MultiXServerlessFunction)
        function3.entry_point = False
        function3.name = "function2"
        function3.handler = "function1"
        function3.regions_and_providers = {"providers": []}
        function3.func_environment_variables = [{"key": "ENV_1", "value": "function3_env_1"}]

        self.builder = WorkflowBuilder()
        self.config = Mock(spec=Config)
        self.config.workflow_name = "test_workflow"
        self.config.workflow_app.functions = {"function1": function1, "function2": function2, "function3": function3}
        self.config.environment_variables = {
            "ENV_1": "global_env_1",
            "ENV_2": "global_env_2",
        }
        self.config.python_version = "3.8"
        self.config.home_regions = []
        self.config.project_dir = "/path/to/project"
        self.config.iam_policy_file = None
        self.config.regions_and_providers = {"providers": []}
        self.config.workflow_app.get_successors.return_value = []

        workflow = self.builder.build_workflow(self.config)

        self.assertEqual(len(workflow._resources), 3)
        built_func1 = workflow._resources[0]
        built_func2 = workflow._resources[1]
        built_func3 = workflow._resources[2]
        self.assertEqual(
            built_func1.environment_variables,
            {
                "ENV_1": "global_env_1",
                "ENV_2": "global_env_2",
            },
        )
        self.assertEqual(
            built_func2.environment_variables,
            {
                "ENV_1": "global_env_1",
                "ENV_2": "global_env_2",
                "ENV_3": "function2_env_3",
            },
        )
        self.assertEqual(
            built_func3.environment_variables,
            {
                "ENV_1": "function3_env_1",
                "ENV_2": "global_env_2",
            },
        )


if __name__ == "__main__":
    unittest.main()
