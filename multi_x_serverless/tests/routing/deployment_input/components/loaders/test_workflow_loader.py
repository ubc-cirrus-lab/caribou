import unittest
from unittest.mock import Mock, patch, MagicMock

import numpy as np
from multi_x_serverless.common.models.remote_client.remote_client import RemoteClient
from multi_x_serverless.routing.deployment_input.components.loaders.workflow_loader import WorkflowLoader
from multi_x_serverless.routing.workflow_config import WorkflowConfig


class TestWorkflowLoader(unittest.TestCase):
    def setUp(self):
        self.workflow_config = MagicMock(spec=WorkflowConfig)
        self.workflow_config.home_region = "home_region"
        self.workflow_config.instances = {
            "image_processing_light-0_0_1-GetInput:entry_point:0": {
                "instance_name": "image_processing_light-0_0_1-GetInput:entry_point:0",
                "regions_and_providers": {"providers": {}},
            }
        }
        self.workflow_data = {
            "workflow_runtime_samples": [5.857085, 5.740116, 7.248474],
            "daily_invocation_counts": {"2024-03-12+0000": 3},
            "start_hop_summary": {"aws:us-east-1": {"3.3527612686157227e-08": [0.52388, 0.514119, 0.519146]}},
            "instance_summary": {
                "image_processing_light-0_0_1-GetInput:entry_point:0": {
                    "invocations": 3,
                    "executions": {"aws:us-east-1": [1.140042781829834, 1.129507303237915, 1.0891644954681396]},
                    "to_instance": {
                        "image_processing_light-0_0_1-Flip:image_processing_light-0_0_1-GetInput_0_0:1": {
                            "invoked": 3,
                            "regions_to_regions": {
                                "aws:us-east-1": {
                                    "aws:us-east-1": {
                                        "transfer_sizes": [2.9960647225379944e-06, 2.9960647225379944e-06],
                                        "transfer_size_to_transfer_latencies": {
                                            "2.9960647225379944e-06": [1.217899, 1.18531]
                                        },
                                    },
                                }
                            },
                            "non_executions": 0,
                            "invocation_probability": 0.9,
                        }
                    },
                },
                "image_processing_light-0_0_1-Flip:image_processing_light-0_0_1-GetInput_0_0:1": {
                    "invocations": 3,
                    "executions": {"aws:us-east-1": [4.638583183288574, 4.554178953170776, 6.073627948760986]},
                    "to_instance": {},
                },
            },
        }

        self.client = Mock(spec=RemoteClient)
        self.loader = WorkflowLoader(self.client, self.workflow_config)

    @patch.object(WorkflowLoader, "_retrieve_workflow_data")
    def test_setup(self, mock_retrieve_workflow_data):
        mock_retrieve_workflow_data.return_value = self.workflow_data
        self.loader.setup({"aws:region1"})
        self.assertEqual(self.loader._workflow_data, self.workflow_data)

    def test_get_home_region(self):
        self.loader._home_region = "provider_1:region_1"
        self.assertEqual(self.loader.get_home_region(), "provider_1:region_1")

    def test_get_runtime_distribution(self):
        self.loader._workflow_data = self.workflow_data
        runtimes = self.loader.get_runtime_distribution(
            "image_processing_light-0_0_1-GetInput:entry_point:0", "aws:us-east-1"
        )
        self.assertEqual(runtimes, [1.140042781829834, 1.129507303237915, 1.0891644954681396])

    def test_get_start_hop_size_distribution(self):
        self.loader._workflow_data = self.workflow_data
        start_hop_size_distribution = self.loader.get_start_hop_size_distribution("aws:us-east-1", "aws:us-east-1")
        self.assertEqual(start_hop_size_distribution, [3.3527612686157227e-08])

    def test_get_start_hop_latency_distribution(self):
        self.loader._workflow_data = self.workflow_data
        start_hop_latency = self.loader.get_start_hop_latency_distribution(
            "aws:us-east-1", "aws:us-east-1", 3.3527612686157227e-08
        )
        self.assertEqual(start_hop_latency, [0.52388, 0.514119, 0.519146])

    def test_get_data_transfer_size_distribution(self):
        self.loader._workflow_data = self.workflow_data
        data_transfer_size = self.loader.get_data_transfer_size_distribution(
            "image_processing_light-0_0_1-GetInput:entry_point:0",
            "image_processing_light-0_0_1-Flip:image_processing_light-0_0_1-GetInput_0_0:1",
            "aws:us-east-1",
            "aws:us-east-1",
        )
        self.assertEqual(data_transfer_size, [2.9960647225379944e-06, 2.9960647225379944e-06])

    def test_get_data_transfer_latency_distribution(self):
        self.loader._workflow_data = self.workflow_data
        data_transfer_latency = self.loader.get_latency_distribution(
            "image_processing_light-0_0_1-GetInput:entry_point:0",
            "image_processing_light-0_0_1-Flip:image_processing_light-0_0_1-GetInput_0_0:1",
            "aws:us-east-1",
            "aws:us-east-1",
            2.9960647225379944e-06,
        )
        self.assertEqual(data_transfer_latency, [1.217899, 1.18531])

    def test_get_invocation_probability(self):
        self.loader._workflow_data = self.workflow_data
        invocation_probability = self.loader.get_invocation_probability(
            "image_processing_light-0_0_1-GetInput:entry_point:0",
            "image_processing_light-0_0_1-Flip:image_processing_light-0_0_1-GetInput_0_0:1",
        )
        self.assertEqual(invocation_probability, 0.9)

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
