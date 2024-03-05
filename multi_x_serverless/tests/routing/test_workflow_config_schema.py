from multi_x_serverless.routing.workflow_config_schema import Instance, WorkflowConfigSchema

import unittest


class TestWorkflowConfigSchema(unittest.TestCase):
    def setUp(self):
        self.instance_data = {
            "instance_name": "instance1",
            "regions_and_providers": {
                "allowed_regions": [
                    {
                        "provider": "provider1",
                        "region": "region2",
                    }
                ],
                "disallowed_regions": [
                    {
                        "provider": "provider1",
                        "region": "region3",
                    }
                ],
                "providers": {
                    "provider1": {
                        "config": {
                            "timeout": 60,
                            "memory": 128,
                        },
                    },
                },
            },
            "succeeding_instances": ["instance2"],
            "preceding_instances": ["instance3"],
        }
        self.maxDiff = None

    def test_instance(self):
        instance = Instance(**self.instance_data)

        self.assertEqual(instance.model_dump(), self.instance_data)

    def test_workflow_config_schema(self):
        workflow_data = {
            "workflow_name": "workflow1",
            "workflow_version": "1.0",
            "workflow_id": "workflow1-1.0",
            "regions_and_providers": {
                "allowed_regions": [
                    {
                        "provider": "provider1",
                        "region": "region2",
                    }
                ],
                "disallowed_regions": [
                    {
                        "provider": "provider1",
                        "region": "region3",
                    }
                ],
                "providers": {
                    "provider1": {
                        "config": {
                            "timeout": 60,
                            "memory": 128,
                        },
                    },
                },
            },
            "instances": {"instance1": self.instance_data},
            "constraints": {
                "hard_resource_constraints": {"runtime": {"value": 100, "type": "absolute"}},
                "soft_resource_constraints": {},
                "priority_order": ["cost", "runtime", "carbon"],
            },
            "start_hops": [{"provider": "provider1", "region": "region1"}],
        }
        WorkflowConfigSchema(**workflow_data)


if __name__ == "__main__":
    unittest.main()
