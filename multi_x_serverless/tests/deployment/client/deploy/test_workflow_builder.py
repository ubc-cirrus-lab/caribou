import os
import unittest
from unittest.mock import Mock, patch
from multi_x_serverless.deployment.client.config import Config
from multi_x_serverless.deployment.client.multi_x_serverless_workflow import MultiXServerlessFunction
from multi_x_serverless.deployment.client.deploy.workflow_builder import WorkflowBuilder


class TestWorkflowBuilder(unittest.TestCase):
    def setUp(self):
        self.builder = WorkflowBuilder()
        self.config = Mock(spec=Config)
        self.config.workflow_name = "test_workflow"
        self.config.workflow_app.functions = {}
        self.config.environment_variables = {}
        self.config.python_version = "3.8"
        self.config.home_regions = []
        self.config.project_dir = "/path/to/project"
        self.config.iam_policy_file = None
        self.config.regions_and_providers = {"providers": []}

    def test_build_workflow_no_entry_point(self):
        with self.assertRaises(RuntimeError):
            self.builder.build_workflow(self.config)

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
        function2.regions_and_providers = {"providers": []}
        function2.environment_variables = {}
        self.config.workflow_app.functions = {"function1": function1, "function2": function2}
        with self.assertRaisesRegex(RuntimeError, "Multiple entry points defined"):
            self.builder.build_workflow(self.config)

    def test_build_workflow_merge_case_self_cycle(self):
        # Create mock functions
        function1 = Mock(spec=MultiXServerlessFunction)
        function1.entry_point = True
        function1.name = "function1"
        function1.handler = "function1"
        function1.regions_and_providers = {}
        function1.is_waiting_for_predecessors = Mock(return_value=False)

        function2 = Mock(spec=MultiXServerlessFunction)
        function2.entry_point = False
        function2.name = "function2"
        function2.handler = "function2"
        function2.regions_and_providers = {"providers": []}
        function2.is_waiting_for_predecessors = Mock(return_value=True)  # This is a merge function

        # Mock the workflow app to return the successors of function1
        self.config.workflow_app.get_successors = Mock(return_value=[function2])

        # Set the functions in the config
        self.config.workflow_app.functions = {"function1": function1, "function2": function2}

        # Call build_workflow
        with self.assertRaisesRegex(
            RuntimeError,
            "Cycle detected: function2 is being visited again",
        ):
            self.builder.build_workflow(self.config)

    def test_build_workflow_merge_case_multiple_incoming(self):
        # Create mock functions
        function1 = Mock(spec=MultiXServerlessFunction)
        function1.entry_point = True
        function1.name = "function1"
        function1.handler = "function1"
        function1.regions_and_providers = {}
        function1.is_waiting_for_predecessors = Mock(return_value=False)

        function2 = Mock(spec=MultiXServerlessFunction)
        function2.entry_point = False
        function2.name = "function2"
        function2.handler = "function2"
        function2.regions_and_providers = {"providers": []}
        function2.is_waiting_for_predecessors = Mock(return_value=True)  # This is a merge function

        # Mock the workflow app to return the successors of function1
        # The first two calls are for the dfs, the next two for the actual successors
        self.config.workflow_app.get_successors = Mock(
            side_effect=[[function2, function2, function2], [], [function2, function2, function2], [], [], []]
        )

        # Set the functions in the config
        self.config.workflow_app.functions = {"function1": function1, "function2": function2}

        # Call build_workflow
        workflow = self.builder.build_workflow(self.config)

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
        function1.is_waiting_for_predecessors = Mock(return_value=False)

        # Mock the workflow app to return the successors of function1
        self.config.workflow_app.get_successors = Mock(side_effect=[[function1], [], []])

        # Set the functions in the config
        self.config.workflow_app.functions = {"function1": function1}

        # Call build_workflow
        with self.assertRaisesRegex(
            RuntimeError,
            "Cycle detected: function1 is being visited again",
        ):
            self.builder.build_workflow(self.config)

    def test_build_workflow_merge_working(self):
        # Create mock functions
        function1 = Mock(spec=MultiXServerlessFunction)
        function1.entry_point = True
        function1.name = "function1"
        function1.handler = "function1"
        function1.regions_and_providers = {}
        function1.is_waiting_for_predecessors = Mock(return_value=False)

        function2 = Mock(spec=MultiXServerlessFunction)
        function2.entry_point = False
        function2.name = "function2"
        function2.handler = "function2"
        function2.regions_and_providers = {"providers": []}
        function2.is_waiting_for_predecessors = Mock(return_value=True)  # This is a merge function

        # Mock the workflow app to return the successors of function1

        self.config.workflow_app.get_successors = Mock(side_effect=[[function2], [], [function2], []])
        # Set the functions in the config
        self.config.workflow_app.functions = {"function1": function1, "function2": function2}

        # Call build_workflow
        workflow = self.builder.build_workflow(self.config)

        self.assertEqual(len(workflow._edges), 1)

    def test_build_workflow_cycle_in_function_calls(self):
        # Create mock functions
        function1 = Mock(spec=MultiXServerlessFunction)
        function1.entry_point = True
        function1.name = "function1"
        function1.handler = "function1"
        function1.regions_and_providers = {}
        function1.is_waiting_for_predecessors = Mock(return_value=False)

        function2 = Mock(spec=MultiXServerlessFunction)
        function2.entry_point = False
        function2.name = "function2"
        function2.handler = "function2"
        function2.regions_and_providers = {"providers": []}
        function2.is_waiting_for_predecessors = Mock(return_value=False)  # This is a merge function

        # Mock the workflow app to return the successors of function1

        self.config.workflow_app.get_successors = Mock(
            side_effect=[[function2], [function1], [function2], [function1], []]
        )
        # Set the functions in the config
        self.config.workflow_app.functions = {"function1": function1, "function2": function2}

        # Call build_workflow and assert the specific error message
        with self.assertRaisesRegex(
            RuntimeError,
            "Cycle detected: function1 is being visited again",
        ):
            self.builder.build_workflow(self.config)

    def test_build_workflow_merge_cycle(self):
        # Create mock functions
        function1 = Mock(spec=MultiXServerlessFunction)
        function1.entry_point = True
        function1.name = "function1"
        function1.handler = "function1"
        function1.regions_and_providers = {}
        function1.is_waiting_for_predecessors = Mock(return_value=False)

        function2 = Mock(spec=MultiXServerlessFunction)
        function2.entry_point = False
        function2.name = "function2"
        function2.handler = "function2"
        function2.regions_and_providers = {"providers": []}
        function2.is_waiting_for_predecessors = Mock(return_value=True)

        self.config.workflow_app.get_successors = Mock(side_effect=[[function2], [function2], []])

        self.config.workflow_app.functions = {"function1": function1, "function2": function2}

        # Call build_workflow
        self.assertRaises(RuntimeError, self.builder.build_workflow, self.config)

    @patch("os.path.join")
    def test_get_function_role_with_policy_file(self, mock_join):
        mock_join.return_value = "/path/to/policy"
        self.config.iam_policy_file = "policy.yml"
        role = self.builder.get_function_role(self.config, "test_function")
        self.assertEqual(role.name, "test_function-role")
        self.assertEqual(role.policy, "/path/to/policy")

    @patch("os.path.join")
    def test_get_function_role_without_policy_file(self, mock_join):
        mock_join.return_value = "/path/to/default_policy"
        role = self.builder.get_function_role(self.config, "test_function")
        self.assertEqual(role.name, "test_function-role")
        self.assertEqual(role.policy, "/path/to/default_policy")


if __name__ == "__main__":
    unittest.main()
