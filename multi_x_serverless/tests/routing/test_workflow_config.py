import unittest
import numpy as np
import json

from unittest.mock import MagicMock

from multi_x_serverless.routing.workflow_config import WorkflowConfig


class TestWorkflowConfig(unittest.TestCase):
    def setUp(self):
        self.workflow_config_dict = {
            "workflow_name": "test_workflow",
            "workflow_version": "1.0",
            "workflow_id": "test_id",
            "regions_and_providers": {
                "allowed_regions": [{"provider": "provider1", "region": "region1"}],
                "providers": {"provider1": {"config": {"timeout": 60, "memory": 128}}},
            },
            "instances": {
                "function1": {
                    "function_name": "function1",
                    "instance_name": "instance1",
                    "regions_and_providers": {"providers": {"provider1": {"config": {"timeout": 120, "memory": 128}}}},
                    "succeeding_instances": [],
                    "preceding_instances": [],
                },
                "function2": {
                    "function_name": "function2",
                    "instance_name": "instance2",
                    "regions_and_providers": {"providers": {"provider1": {"config": {"timeout": 120, "memory": 128}}}},
                    "succeeding_instances": [],
                    "preceding_instances": [],
                },
            },
            "constraints": {
                "hard_resource_constraints": {},
                "soft_resource_constraints": {},
                "priority_order": [],
            },
            "home_region": {"provider": "provider1", "region": "region1"},
        }
        self.workflow_config = WorkflowConfig(self.workflow_config_dict)
        self.maxDiff = None

    def test_workflow_name(self):
        self.assertEqual(self.workflow_config.workflow_name, "test_workflow")

    def test_workflow_version(self):
        self.assertEqual(self.workflow_config.workflow_version, "1.0")

    def test_workflow_id(self):
        self.assertEqual(self.workflow_config.workflow_id, "test_id")

    def test_to_json(self):
        self.assertEqual(self.workflow_config.to_json(), json.dumps(self.workflow_config_dict))

    def test_regions_and_providers(self):
        self.assertEqual(
            self.workflow_config.regions_and_providers,
            {
                "providers": {"provider1": {"config": {"timeout": 60, "memory": 128}}},
                "allowed_regions": ["provider1:region1"],
            },
        )

    def test_instances(self):
        self.assertEqual(
            self.workflow_config.instances,
            {
                "function1": {
                    "function_name": "function1",
                    "instance_name": "instance1",
                    "preceding_instances": [],
                    "regions_and_providers": {"providers": {"provider1": {"config": {"memory": 128, "timeout": 120}}}},
                    "succeeding_instances": [],
                },
                "function2": {
                    "function_name": "function2",
                    "instance_name": "instance2",
                    "preceding_instances": [],
                    "regions_and_providers": {"providers": {"provider1": {"config": {"memory": 128, "timeout": 120}}}},
                    "succeeding_instances": [],
                },
            },
        )

    def test_constraints(self):
        self.assertEqual(
            self.workflow_config.constraints,
            {"hard_resource_constraints": {}, "priority_order": [], "soft_resource_constraints": {}},
        )

    def test_deployment_algorithm(self):
        # Mock the _lookup method to return a specific value
        self.workflow_config._lookup = MagicMock()
        self.workflow_config._lookup.return_value = "fine_grained_deployment_algorithm"

        # Call the property
        result = self.workflow_config.deployment_algorithm

        # Define the expected result
        expected_result = "stochastic_heuristic_deployment_algorithm"

        # Assert that the result matches the expected result
        self.assertEqual(result, expected_result)

    def test_home_region(self):
        self.assertEqual(self.workflow_config.home_region, "provider1:region1")


if __name__ == "__main__":
    unittest.main()
