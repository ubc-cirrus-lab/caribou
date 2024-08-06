import unittest
from unittest.mock import Mock, patch, MagicMock
from caribou.common.models.remote_client.remote_client import RemoteClient
from caribou.deployment_solver.deployment_input.components.loaders.workflow_loader import WorkflowLoader
from caribou.deployment_solver.workflow_config import WorkflowConfig


class TestWorkflowLoader(unittest.TestCase):
    def setUp(self):
        # Mock workflow config
        self.workflow_config = MagicMock(spec=WorkflowConfig)
        self.workflow_config.home_region = "aws:ca-west-1"
        self.workflow_config.instances = {
            "simple_call-0_0_1-f1:entry_point:0": {
                "instance_name": "simple_call-0_0_1-f1:entry_point:0",
                "regions_and_providers": {"providers": {}},
            },
            "simple_call-0_0_1-f2:simple_call-0_0_1-f1_0_0:1": {
                "instance_name": "simple_call-0_0_1-f2:simple_call-0_0_1-f1_0_0:1",
                "regions_and_providers": {"providers": {}},
            },
        }

        # Example input data, matching the new format
        self.workflow_data = {
            "workflow_runtime_samples": [5.001393, 5.001395, 5.511638],
            "daily_invocation_counts": {"2024-08-01+0000": 18},
            "start_hop_summary": {
                "invoked": 13,
                "retrieved_wpd_at_function": 11,
                "wpd_at_function_probability": 0.8461538461538461,
                "workflow_placement_decision_size_gb": 1.5972182154655457e-06,
                "from_client": {
                    "transfer_sizes_gb": [1.9846484065055847e-06, 1.9846484065055847e-06, 2.1280720829963684e-06],
                    "received_region": {
                        "aws:ca-west-1": {
                            "transfer_size_gb_to_transfer_latencies_s": {
                                "9.5367431640625e-06": [0.31179, 0.16143, 0.250207]
                            },
                            "best_fit_line": {
                                "slope_s": 0.0,
                                "intercept_s": 0.236685125,
                                "min_latency_s": 0.16567958749999998,
                                "max_latency_s": 0.3076906625,
                            },
                        }
                    },
                },
            },
            "instance_summary": {
                "simple_call-0_0_1-f1:entry_point:0": {
                    "invocations": 13,
                    "cpu_utilization": 0.060168300407512976,
                    "executions": {
                        "at_region": {
                            "aws:ca-west-1": {
                                "cpu_utilization": 0.0563360316574529,
                                "durations_s": [5.002, 5.002, 5.025],
                                "auxiliary_data": {
                                    "5.01": [[3.6343932151794434e-05, 0.001], [1.291465014219284e-05, 0.001]]
                                },
                            }
                        }
                    },
                    "to_instance": {
                        "simple_call-0_0_1-f2:simple_call-0_0_1-f1_0_0:1": {
                            "invoked": 13,
                            "invocation_probability": 1.0,
                            "transfer_sizes_gb": [1.9157305359840393e-06],
                            "regions_to_regions": {
                                "aws:ca-west-1": {
                                    "aws:ca-west-1": {
                                        "transfer_size_gb_to_transfer_latencies_s": {
                                            "9.5367431640625e-06": [0.27353, 0.175446, 0.188676]
                                        },
                                        "best_fit_line": {
                                            "slope_s": 0.0,
                                            "intercept_s": 0.220738625,
                                            "min_latency_s": 0.15451703749999998,
                                            "max_latency_s": 0.2869602125,
                                        },
                                    }
                                }
                            },
                        }
                    },
                }
            },
        }

        self.client = Mock(spec=RemoteClient)
        self.loader = WorkflowLoader(self.client, self.workflow_config)

    @patch.object(WorkflowLoader, "_retrieve_workflow_data")
    def test_setup(self, mock_retrieve_workflow_data):
        mock_retrieve_workflow_data.return_value = self.workflow_data
        self.loader.setup("workflow_id")
        self.assertEqual(self.loader.get_workflow_data(), self.workflow_data)

    def test_get_home_region(self):
        self.assertEqual(self.loader.get_home_region(), "aws:ca-west-1")

    def test_get_runtime_distribution(self):
        self.loader.set_workflow_data(self.workflow_data)
        runtimes = self.loader.get_runtime_distribution(
            "simple_call-0_0_1-f1:entry_point:0", "aws:ca-west-1", is_redirector=False
        )
        self.assertEqual(runtimes, [5.002, 5.002, 5.025])

    def test_get_start_hop_size_distribution(self):
        self.loader.set_workflow_data(self.workflow_data)
        start_hop_size_distribution = self.loader.get_start_hop_size_distribution()
        self.assertEqual(
            start_hop_size_distribution, [1.9846484065055847e-06, 1.9846484065055847e-06, 2.1280720829963684e-06]
        )

    def test_get_start_hop_latency_distribution(self):
        self.loader.set_workflow_data(self.workflow_data)
        start_hop_latency = self.loader.get_start_hop_latency_distribution("aws:ca-west-1", 9.5367431640625e-06)
        self.assertEqual(start_hop_latency, [0.31179, 0.16143, 0.250207])

    def test_get_data_transfer_size_distribution(self):
        self.loader.set_workflow_data(self.workflow_data)
        data_transfer_size = self.loader.get_data_transfer_size_distribution(
            "simple_call-0_0_1-f1:entry_point:0", "simple_call-0_0_1-f2:simple_call-0_0_1-f1_0_0:1"
        )
        self.assertEqual(data_transfer_size, [1.9157305359840393e-06])

    def test_get_data_transfer_latency_distribution(self):
        self.loader.set_workflow_data(self.workflow_data)
        data_transfer_latency = self.loader.get_latency_distribution(
            "simple_call-0_0_1-f1:entry_point:0",
            "simple_call-0_0_1-f2:simple_call-0_0_1-f1_0_0:1",
            "aws:ca-west-1",
            "aws:ca-west-1",
            9.5367431640625e-06,
        )
        self.assertEqual(data_transfer_latency, [0.27353, 0.175446, 0.188676])

    def test_get_invocation_probability(self):
        self.loader.set_workflow_data(self.workflow_data)
        invocation_probability = self.loader.get_invocation_probability(
            "simple_call-0_0_1-f1:entry_point:0", "simple_call-0_0_1-f2:simple_call-0_0_1-f1_0_0:1"
        )
        self.assertEqual(invocation_probability, 1.0)

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
