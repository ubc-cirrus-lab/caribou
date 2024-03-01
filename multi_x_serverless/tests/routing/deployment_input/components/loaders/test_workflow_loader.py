import unittest
from unittest.mock import Mock, patch, MagicMock
from multi_x_serverless.common.models.remote_client.remote_client import RemoteClient
from multi_x_serverless.routing.deployment_input.components.loaders.workflow_loader import WorkflowLoader
from multi_x_serverless.routing.workflow_config import WorkflowConfig


class TestWorkflowLoader(unittest.TestCase):
    def setUp(self):
        self.workflow_config = MagicMock(spec=WorkflowConfig)
        self.workflow_config.start_hops = 'start_hops'
        self.workflow_config.instances = [{"instance_name": "instance_1", "regions_and_providers": {"providers": {}}}]
        self.workflow_data = {
            "instance_1": {
                "projected_monthly_invocations": 12.5,
                "execution_summary": {
                    "provider_1:region_1": {"runtime_distribution": [26.0]},
                    "provider_1:region_2": {"runtime_distribution": [26.5]},
                },
                "invocation_summary": {
                    "instance_2": {
                        "probability_of_invocation": 0.8,
                        "data_transfer_size_distribution": [0.0007],
                        "transmission_summary": {
                            "provider_1:region_1": {
                                "provider_1:region_1": {"latency_distribution": [0.00125]},
                                "provider_1:region_2": {"latency_distribution": [0.125]},
                            },
                            "provider_1:region_2": {
                                "provider_1:region_1": {"latency_distribution": [0.095]}
                            },
                        },
                    }
                },
            },
            "instance_2": {
                "projected_monthly_invocations": 11.25,
                "execution_summary": {
                    "provider_1:region_1": {"runtime_distribution": [12.5]},
                    "provider_1:region_2": {"runtime_distribution": [13.5]},
                },
                "invocation_summary": {},
            },
        }
        self.client = Mock(spec=RemoteClient)
        self.loader = WorkflowLoader(self.client, self.workflow_config)

    @patch.object(
        WorkflowLoader, "_retrieve_workflow_data"
    )
    def test_setup(self, mock_retrieve_workflow_data):
        mock_retrieve_workflow_data.return_value = self.workflow_data
        self.loader.setup({"aws:region1"})
        self.assertEqual(self.loader._workflow_data, self.workflow_data)

    def test_get_home_region(self):
        self.loader._home_region = "provider_1:region_1"
        self.assertEqual(self.loader.get_home_region(), "provider_1:region_1")

    def test_get_runtime_distribution(self):
        self.loader._workflow_data = self.workflow_data
        runtime = self.loader.get_runtime_distribution("instance_1", "provider_1:region_1")
        self.assertEqual(runtime, [26.0])

    def test_get_latency_distribution(self):
        self.loader._workflow_data = self.workflow_data
        latency = self.loader.get_latency_distribution("instance_1", "instance_2", "provider_1:region_1", "provider_1:region_1")
        self.assertEqual(latency, [0.00125])

    def test_get_data_transfer_size_distribution(self):
        self.loader._workflow_data = self.workflow_data
        data_transfer_size = self.loader.get_data_transfer_size_distribution("instance_1", "instance_2")
        self.assertEqual(data_transfer_size, [0.0007])

    def test_get_invocation_probability(self):
        self.loader._workflow_data = self.workflow_data
        invocation_probability = self.loader.get_invocation_probability("instance_1", "instance_2")
        self.assertEqual(invocation_probability, 0.8)

    def test_get_projected_monthly_invocations(self):
        self.loader._workflow_data = self.workflow_data
        projected_monthly_invocations = self.loader.get_projected_monthly_invocations("instance_1")
        self.assertEqual(projected_monthly_invocations, 12.5)

    def test_get_vcpu(self):
        self.loader._instances_regions_and_providers = {"instance_1": {"provider_1": {"config": {"vcpu": 2}}}}
        vcpu = self.loader.get_vcpu("instance_1", "provider_1")
        self.assertEqual(vcpu, 2)

    def test_get_memory(self):
        self.loader._instances_regions_and_providers = {"instance_1": {"provider_1": {"config": {"memory": 1024}}}}
        memory = self.loader.get_memory("instance_1", "provider_1")
        self.assertEqual(memory, 1024)

    def test_get_architecture(self):
        self.loader._instances_regions_and_providers = {
            "instance_1": {"provider_1": {"config": {"architecture": "x86_64"}}}
        }
        architecture = self.loader.get_architecture("instance_1", "provider_1")
        self.assertEqual(architecture, "x86_64")


if __name__ == "__main__":
    unittest.main()
