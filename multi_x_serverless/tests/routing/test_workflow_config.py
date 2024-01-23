import unittest
import numpy as np
import json

from multi_x_serverless.routing.workflow_config import WorkflowConfig


class TestWorkflowConfig(unittest.TestCase):
    def setUp(self):
        self.workflow_config_dict = {
            "workflow_name": "test_workflow",
            "workflow_version": "1.0",
            "workflow_id": "test_id",
            "regions_and_providers": {"providers": {"aws": {"config": {"timeout": 60, "memory": 128}}}},
            "instances": [
                {
                    "function_name": "function1",
                    "instance_name": "instance1",
                    "regions_and_providers": {"providers": {"aws": {"config": {"timeout": 120, "memory": 128}}}},
                    "succeeding_instances": [],
                    "preceding_instances": [],
                },
                {
                    "function_name": "function2",
                    "instance_name": "instance2",
                    "regions_and_providers": {"providers": {"aws": {"config": {"timeout": 120, "memory": 128}}}},
                    "succeeding_instances": [],
                    "preceding_instances": [],
                },
            ],
            "constraints": {
                "hard_resource_constraints": {},
                "soft_resource_constraints": {},
                "priority_order": [],
            },
            "start_hops": [{"provider": "provider1", "region": "region1"}],
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

    def test_resolve_functions(self):
        expected_functions = np.array(["function2", "function1"])
        np.testing.assert_array_equal(self.workflow_config.resolve_functions(), expected_functions)

    def test_regions_and_providers(self):
        self.assertEqual(
            self.workflow_config.regions_and_providers,
            {"providers": {"aws": {"config": {"memory": 128, "timeout": 60}}}},
        )

    def test_instances(self):
        self.assertEqual(
            self.workflow_config.instances,
            [
                {
                    "function_name": "function1",
                    "instance_name": "instance1",
                    "preceding_instances": [],
                    "regions_and_providers": {"providers": {"aws": {"config": {"memory": 128, "timeout": 120}}}},
                    "succeeding_instances": [],
                },
                {
                    "function_name": "function2",
                    "instance_name": "instance2",
                    "preceding_instances": [],
                    "regions_and_providers": {"providers": {"aws": {"config": {"memory": 128, "timeout": 120}}}},
                    "succeeding_instances": [],
                },
            ],
        )

    def test_constraints(self):
        self.assertEqual(
            self.workflow_config.constraints,
            {"hard_resource_constraints": {}, "priority_order": [], "soft_resource_constraints": {}},
        )

    def test_start_hops(self):
        self.assertEqual(self.workflow_config.start_hops, {"provider": "provider1", "region": "region1"})


if __name__ == "__main__":
    unittest.main()
