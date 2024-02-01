import unittest
from unittest.mock import Mock, patch
from multi_x_serverless.deployment.common.config.config import Config
from multi_x_serverless.deployment.client.multi_x_serverless_workflow import MultiXServerlessFunction
from multi_x_serverless.deployment.common.deploy.workflow_builder import WorkflowBuilder
from multi_x_serverless.common.provider import Provider

import tempfile
import os
import shutil


class TestWorkflowBuilder(unittest.TestCase):
    def setUp(self):
        self.project_dir = tempfile.mkdtemp()
        self.builder = WorkflowBuilder()
        self.config = Mock(spec=Config)
        self.config.workflow_name = "test_workflow"
        self.config.workflow_version = "0.0.1"
        self.config.workflow_app.functions = {}
        self.config.workflow_app.name = "test_workflow"
        self.config.workflow_app.version = "0.0.1"
        self.config.environment_variables = {}
        self.config.python_version = "3.8"
        self.config.home_regions = []
        self.config.project_dir = self.project_dir
        self.config.regions_and_providers = {"providers": {}}

        self.policy_file = os.path.join(self.project_dir, "policy.json")

        os.mkdir(os.path.join(self.project_dir, ".multi-x-serverless"))

        with open(self.policy_file, "w") as f:
            f.write('{"provider1": {"Version": "2012-10-17"}}')

        with open(os.path.join(self.project_dir, ".multi-x-serverless", "policy.json"), "w") as f:
            f.write('{ "workflow_name": "test_workflow" }')

        self.config.iam_policy_file = "policy.json"

    def tearDown(self):
        shutil.rmtree(self.project_dir)

    def test_build_workflow_no_entry_point(self):
        function1 = Mock(spec=MultiXServerlessFunction)
        function1.entry_point = False
        function1.name = "function1"
        function1.handler = "function1"
        function1.regions_and_providers = {}
        function1.environment_variables = {}
        self.config.workflow_app.functions = {"function1": function1}
        with self.assertRaises(RuntimeError):
            self.builder.build_workflow(self.config, [{"provider": "provider1", "region": "region1"}])

    def test_build_workflow_multiple_entry_points(self):
        function1 = Mock(spec=MultiXServerlessFunction)
        function1.entry_point = True
        function1.name = "function1"
        function1.handler = "function1"
        function1.regions_and_providers = {}
        function1.environment_variables = {}
        function2 = Mock(spec=MultiXServerlessFunction)
        function2.entry_point = True
        function2.name = "function2"
        function2.handler = "function1"
        function2.regions_and_providers = {"providers": {}}
        function2.environment_variables = {}
        self.config.workflow_app.functions = {"function1": function1, "function2": function2}

        with self.assertRaisesRegex(RuntimeError, "Multiple entry points defined"):
            self.builder.build_workflow(self.config, [{"provider": "provider1", "region": "region1"}])

    def test_build_workflow_sync_case_self_cycle(self):
        # Create mock functions
        function1 = Mock(spec=MultiXServerlessFunction)
        function1.entry_point = True
        function1.name = "function1"
        function1.handler = "function1"
        function1.regions_and_providers = {}
        function1.environment_variables = []
        function1.is_waiting_for_predecessors = Mock(return_value=False)

        function2 = Mock(spec=MultiXServerlessFunction)
        function2.entry_point = False
        function2.name = "function2"
        function2.handler = "function2"
        function2.regions_and_providers = {"providers": {}}
        function2.environment_variables = []
        function2.is_waiting_for_predecessors = Mock(return_value=True)

        # Mock the workflow app to return the successors of function1
        self.config.workflow_app.get_successors = Mock(return_value=[function2])

        # Set the functions in the config
        self.config.workflow_app.functions = {"function1": function1, "function2": function2}

        # Call build_workflow
        with self.assertRaisesRegex(
            RuntimeError,
            "Cycle detected: test_workflow-0_0_1-function2_provider1-region1 is being visited again",
        ):
            self.builder.build_workflow(self.config, [{"provider": "provider1", "region": "region1"}])

    def test_build_workflow_sync_case_multiple_incoming(self):
        # Create mock functions
        function1 = Mock(spec=MultiXServerlessFunction)
        function1.entry_point = True
        function1.name = "function1"
        function1.handler = "function1"
        function1.regions_and_providers = {}
        function1.environment_variables = []
        function1.is_waiting_for_predecessors = Mock(return_value=False)

        function2 = Mock(spec=MultiXServerlessFunction)
        function2.entry_point = False
        function2.name = "function2"
        function2.handler = "function2"
        function2.regions_and_providers = {"providers": {}}
        function2.environment_variables = []
        function2.is_waiting_for_predecessors = Mock(return_value=True)

        # Mock the workflow app to return the successors of function1
        # The first two calls are for the dfs, the next two for the actual successors
        self.config.workflow_app.get_successors = Mock(
            side_effect=[[function2, function2, function2], [], [function2, function2, function2], [], [], []]
        )

        # Set the functions in the config
        self.config.workflow_app.functions = {"function1": function1, "function2": function2}

        # Call build_workflow
        workflow = self.builder.build_workflow(self.config, [{"provider": "provider1", "region": "region1"}])

        self.assertEqual(len(workflow._edges), 3)

    def test_cycle_detection(self):
        # Create mock functions
        function1 = Mock(spec=MultiXServerlessFunction)
        function1.name = "function1"

        function2 = Mock(spec=MultiXServerlessFunction)
        function2.name = "function2"

        # Create a mock config
        config = Mock(spec=Config)
        config.workflow_app.get_successors = Mock(side_effect=[[function2], [function1]])

        # This should raise a RuntimeError because there is a cycle
        with self.assertRaises(RuntimeError) as context:
            self.builder._cycle_check(function1, config)

        self.assertTrue("Cycle detected: function1 is being visited again" == str(context.exception))

    def test_cycle_detection_no_cycle(self):
        # Create mock functions
        function1 = Mock(spec=MultiXServerlessFunction)
        function1.name = "function1"

        function2 = Mock(spec=MultiXServerlessFunction)
        function2.name = "function2"

        function3 = Mock(spec=MultiXServerlessFunction)
        function3.name = "function3"

        # Create a mock config
        config = Mock(spec=Config)
        config.workflow_app.get_successors = Mock(side_effect=[[function2], [function3], []])

        # This should not raise a RuntimeError because there is no cycle
        self.builder._cycle_check(function1, config)

    def test_build_workflow_self_call(self):
        # Create mock functions
        function1 = Mock(spec=MultiXServerlessFunction)
        function1.entry_point = True
        function1.name = "function1"
        function1.handler = "function1"
        function1.regions_and_providers = {}
        function1.environment_variables = []
        function1.is_waiting_for_predecessors = Mock(return_value=False)

        # Mock the workflow app to return the successors of function1
        self.config.workflow_app.get_successors = Mock(side_effect=[[function1], [], []])

        # Set the functions in the config
        self.config.workflow_app.functions = {"function1": function1}

        # Call build_workflow
        with self.assertRaisesRegex(
            RuntimeError,
            "Cycle detected: test_workflow-0_0_1-function1_provider1-region1 is being visited again",
        ):
            self.builder.build_workflow(self.config, [{"provider": "provider1", "region": "region1"}])

    def test_build_workflow_sync_working(self):
        # Create mock functions
        function1 = Mock(spec=MultiXServerlessFunction)
        function1.entry_point = True
        function1.name = "function1"
        function1.handler = "function1"
        function1.regions_and_providers = {}
        function1.environment_variables = []
        function1.is_waiting_for_predecessors = Mock(return_value=False)

        function2 = Mock(spec=MultiXServerlessFunction)
        function2.entry_point = False
        function2.name = "function2"
        function2.handler = "function2"
        function2.regions_and_providers = {"providers": {}}
        function2.environment_variables = []
        function2.is_waiting_for_predecessors = Mock(return_value=True)

        # Mock the workflow app to return the successors of function1

        self.config.workflow_app.get_successors = Mock(side_effect=[[function2], [], [function2], []])
        # Set the functions in the config
        self.config.workflow_app.functions = {"function1": function1, "function2": function2}

        # Call build_workflow
        workflow = self.builder.build_workflow(self.config, [{"provider": "provider1", "region": "region1"}])

        self.assertEqual(len(workflow._edges), 1)

    def test_build_workflow_cycle_in_function_calls(self):
        # Create mock functions
        function1 = Mock(spec=MultiXServerlessFunction)
        function1.entry_point = True
        function1.name = "function1"
        function1.handler = "function1"
        function1.regions_and_providers = {}
        function1.environment_variables = []
        function1.is_waiting_for_predecessors = Mock(return_value=False)

        function2 = Mock(spec=MultiXServerlessFunction)
        function2.entry_point = False
        function2.name = "function2"
        function2.handler = "function2"
        function2.regions_and_providers = {"providers": {}}
        function2.environment_variables = []
        function2.is_waiting_for_predecessors = Mock(return_value=False)

        # Mock the workflow app to return the successors of function1

        self.config.workflow_app.get_successors = Mock(
            side_effect=[[function2], [function1], [function2], [function1], []]
        )
        # Set the functions in the config
        self.config.workflow_app.functions = {"function1": function1, "function2": function2}

        # Call build_workflow and assert the specific error message
        with self.assertRaisesRegex(
            RuntimeError,
            "Cycle detected: test_workflow-0_0_1-function1_provider1-region1 is being visited again",
        ):
            self.builder.build_workflow(self.config, [{"provider": "provider1", "region": "region1"}])

    def test_build_workflow_sync_cycle(self):
        # Create mock functions
        function1 = Mock(spec=MultiXServerlessFunction)
        function1.entry_point = True
        function1.name = "function1"
        function1.handler = "function1"
        function1.regions_and_providers = {}
        function1.environment_variables = []
        function1.is_waiting_for_predecessors = Mock(return_value=False)

        function2 = Mock(spec=MultiXServerlessFunction)
        function2.entry_point = False
        function2.name = "function2"
        function2.handler = "function2"
        function2.regions_and_providers = {"providers": {}}
        function2.environment_variables = []
        function2.is_waiting_for_predecessors = Mock(return_value=True)

        self.config.workflow_app.get_successors = Mock(side_effect=[[function2], [function2], []])

        self.config.workflow_app.functions = {"function1": function1, "function2": function2}

        # Call build_workflow
        self.assertRaises(RuntimeError, self.builder.build_workflow, self.config, [])

    @patch("os.path.join")
    def test_get_function_role_with_policy_file(self, mock_join):
        mock_join.return_value = self.policy_file
        self.config.iam_policy_file = "policy.yml"
        role = self.builder.get_function_role(self.config, "test_function")
        self.assertEqual(role.name, "test_function-role")
        self.assertEqual(role.get_policy(Provider.TEST_PROVIDER1), '{"Version": "2012-10-17"}')

    @patch("os.path.join")
    def test_get_function_role_without_policy_file(self, mock_join):
        mock_join.return_value = self.policy_file
        role = self.builder.get_function_role(self.config, "test_function")
        self.assertEqual(role.name, "test_function-role")
        self.assertEqual(role.get_policy(Provider.TEST_PROVIDER1), '{"Version": "2012-10-17"}')

    def test_build_workflow_and_config_name_not_equals(self):
        self.builder = WorkflowBuilder()
        config = Mock(spec=Config)
        config.workflow_name = "test_workflow"
        config.workflow_app.name = "not_test_workflow"

        with self.assertRaisesRegex(RuntimeError, "Workflow name in config and workflow app must match"):
            self.builder.build_workflow(config, [{"provider": "provider1", "region": "region1"}])

    def test_build_func_environment_variables(self):
        # function 1 (empty function level environment variables)
        function1 = Mock(spec=MultiXServerlessFunction)
        function1.entry_point = True
        function1.name = "function1"
        function1.handler = "function1"
        function1.regions_and_providers = {}
        function1.environment_variables = []

        # function 2 (no overlap with global environment variables)
        function2 = Mock(spec=MultiXServerlessFunction)
        function2.entry_point = False
        function2.name = "function2"
        function2.handler = "function1"
        function2.regions_and_providers = {"providers": {}}
        function2.environment_variables = [{"key": "ENV_3", "value": "function2_env_3"}]

        # function 3 (overlap with global environment variables)
        function3 = Mock(spec=MultiXServerlessFunction)
        function3.entry_point = False
        function3.name = "function2"
        function3.handler = "function1"
        function3.regions_and_providers = {"providers": {}}
        function3.environment_variables = [{"key": "ENV_1", "value": "function3_env_1"}]

        self.builder = WorkflowBuilder()
        config = Mock(spec=Config)
        config.workflow_name = "test_workflow"
        config.workflow_version = "0.0.1"
        config.workflow_app.functions = {"function1": function1, "function2": function2, "function3": function3}
        config.environment_variables = {
            "ENV_1": "global_env_1",
            "ENV_2": "global_env_2",
        }
        config.python_version = "3.8"
        config.home_regions = []
        config.project_dir = self.project_dir
        config.regions_and_providers = {"providers": {}}
        config.workflow_app.get_successors.return_value = []
        config.workflow_app.name = "test_workflow"
        config.workflow_app.version = "0.0.1"
        config.iam_policy_file = "policy.json"

        workflow = self.builder.build_workflow(config, [{"provider": "provider1", "region": "region1"}])

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

    def test_no_regions_and_providers(self):
        config = Mock(spec=Config)
        config.regions_and_providers = {"providers": {"provider1": "value"}}
        config.home_regions = [{"provider": "provider1", "region": "region1"}]
        result = self.builder._merge_and_verify_regions_and_providers({}, config)
        self.assertEqual(result, {"providers": {"provider1": "value"}})

    def test_providers_only(self):
        config = Mock(spec=Config)
        config.regions_and_providers = {"providers": {"provider1": "value"}}
        config.home_regions = [{"provider": "provider1", "region": "region1"}]
        regions_and_providers = {"providers": {"provider2": "value2"}}
        result = self.builder._merge_and_verify_regions_and_providers(regions_and_providers, config)
        self.assertEqual(result, {"providers": {"provider2": "value2"}})

    def test_allowed_regions_only(self):
        config = Mock(spec=Config)
        config.regions_and_providers = {"providers": {"provider1": "value"}}
        config.home_regions = [{"provider": "provider1", "region": "region1"}]
        regions_and_providers = {"allowed_regions": [{"provider": "provider1", "region": "value"}]}
        result = self.builder._merge_and_verify_regions_and_providers(regions_and_providers, config)
        self.assertEqual(
            result,
            {"providers": {"provider1": "value"}, "allowed_regions": [{"provider": "provider1", "region": "value"}]},
        )

    def test_disallowed_regions_only(self):
        config = Mock(spec=Config)
        config.regions_and_providers = {"providers": {"provider1": "value"}}
        config.home_regions = [{"provider": "provider1", "region": "region1"}]
        regions_and_providers = {"disallowed_regions": [{"provider": "provider1", "region": "value"}]}
        result = self.builder._merge_and_verify_regions_and_providers(regions_and_providers, config)
        self.assertEqual(
            result,
            {
                "providers": {"provider1": "value"},
                "disallowed_regions": [{"provider": "provider1", "region": "value"}],
            },
        )

    def test_both_allowed_and_disallowed_regions(self):
        config = Mock(spec=Config)
        config.regions_and_providers = {"providers": {"provider1": "value"}}
        config.home_regions = [{"provider": "provider1", "region": "region1"}]
        regions_and_providers = {
            "allowed_regions": [{"provider": "provider1", "region": "value"}],
            "disallowed_regions": [{"provider": "provider1", "region": "value1"}],
        }
        result = self.builder._merge_and_verify_regions_and_providers(regions_and_providers, config)
        self.assertEqual(
            result,
            {
                "providers": {"provider1": "value"},
                "allowed_regions": [{"provider": "provider1", "region": "value"}],
                "disallowed_regions": [{"provider": "provider1", "region": "value1"}],
            },
        )

    def test_allowed_regions_not_list(self):
        config = Mock(spec=Config)
        config.regions_and_providers = {"providers": {"provider1": "value"}}
        regions_and_providers = {"allowed_regions": "not a list"}
        with self.assertRaises(RuntimeError, msg="allowed_regions must be a list"):
            self.builder._merge_and_verify_regions_and_providers(regions_and_providers, config)

    def test_allowed_regions_missing_provider_or_region(self):
        config = Mock(spec=Config)
        config.regions_and_providers = {"providers": {"provider1": "value"}}
        regions_and_providers = {"allowed_regions": [{"provider": "provider1"}]}
        with self.assertRaises(
            RuntimeError, msg="Region {'provider': 'provider1'} must have both provider and region defined"
        ):
            self.builder._merge_and_verify_regions_and_providers(regions_and_providers, config)

    def test_disallowed_regions_not_list(self):
        config = Mock(spec=Config)
        config.regions_and_providers = {"providers": {"provider1": "value"}}
        regions_and_providers = {"disallowed_regions": "not a list"}
        with self.assertRaises(RuntimeError, msg="disallowed_regions must be a list"):
            self.builder._merge_and_verify_regions_and_providers(regions_and_providers, config)

    def test_disallowed_regions_missing_provider_or_region(self):
        config = Mock(spec=Config)
        config.regions_and_providers = {"providers": {"provider1": "value"}}
        regions_and_providers = {"disallowed_regions": [{"provider": "provider1"}]}
        with self.assertRaises(
            RuntimeError, msg="Region {'provider': 'provider1'} must have both provider and region defined"
        ):
            self.builder._merge_and_verify_regions_and_providers(regions_and_providers, config)

    def test_region_both_allowed_and_disallowed(self):
        config = Mock(spec=Config)
        config.regions_and_providers = {"providers": {"provider1": "value"}}
        regions_and_providers = {
            "allowed_regions": [{"provider": "provider1", "region": "value"}],
            "disallowed_regions": [{"provider": "provider1", "region": "value"}],
        }
        with self.assertRaises(
            RuntimeError,
            msg="Region {'provider': 'provider1', 'region': 'value'} cannot be both allowed and disallowed",
        ):
            self.builder._merge_and_verify_regions_and_providers(regions_and_providers, config)

    def test_region_both_home_and_disallowed(self):
        config = Mock(spec=Config)
        config.regions_and_providers = {"providers": {"provider1": "value"}}
        config.home_regions = [{"provider": "provider1", "region": "value"}]
        regions_and_providers = {"disallowed_regions": [{"provider": "provider1", "region": "value"}]}
        with self.assertRaises(
            RuntimeError, msg="Region {'provider': 'provider1', 'region': 'value'} cannot be both home and disallowed"
        ):
            self.builder._merge_and_verify_regions_and_providers(regions_and_providers, config)


if __name__ == "__main__":
    unittest.main()
