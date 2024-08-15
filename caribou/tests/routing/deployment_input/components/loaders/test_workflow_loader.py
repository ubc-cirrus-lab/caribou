import unittest
from unittest.mock import MagicMock, patch
from caribou.common.models.remote_client.remote_client import RemoteClient
from caribou.deployment_solver.workflow_config import WorkflowConfig
from caribou.common.constants import SNS_SIZE_DEFAULT, SYNC_SIZE_DEFAULT
from caribou.deployment_solver.deployment_input.components.loaders.workflow_loader import WorkflowLoader


class TestWorkflowLoader(unittest.TestCase):
    def setUp(self):
        self.client = MagicMock(spec=RemoteClient)
        self.workflow_config = MagicMock(spec=WorkflowConfig)
        self.workflow_config.home_region = "aws:ca-west-1"
        self.workflow_config.instances = {
            "simple_call-0_0_2-f1:entry_point:0": {
                "instance_name": "simple_call-0_0_2-f1:entry_point:0",
                "regions_and_providers": {
                    "providers": {"aws": {"config": {"vcpu": 2, "memory": 2048, "architecture": "x86_64"}}}
                },
            }
        }

        self.loader = WorkflowLoader(self.client, self.workflow_config)

        # Example workflow data that matches the format expected
        self.workflow_data = {
            "workflow_runtime_samples": [5.068629, 5.068141, 5.067939, 5.068179, 5.068048],
            "daily_invocation_counts": {"2024-08-08+0000": 44},
            "start_hop_summary": {
                "invoked": 40,
                "workflow_placement_decision_size_gb": 1.7611309885978699e-06,
                "wpd_at_function_probability": 1.0,
                "at_redirector": {
                    "simple_call-0_0_2-f1:entry_point:0": {
                        "invocations": 30,
                        "cpu_utilization": 0.5046842067960691,
                        "executions": {
                            "at_region": {
                                "aws:us-east-1": {
                                    "durations_s": [0.457, 0.465, 0.683, 0.456, 0.451],
                                    "auxiliary_data": {
                                        "0.46": [[2.7985312044620514e-05, 0.068], [2.796109765768051e-05, 0.068]]
                                    },
                                }
                            }
                        },
                    }
                },
                "from_client": {
                    "transfer_sizes_gb": [1.9185245037078857e-07] * 5,
                    "received_region": {
                        "aws:us-east-1": {
                            "transfer_size_gb_to_transfer_latencies_s": {
                                "9.5367431640625e-06": [0.42088, 0.41369, 0.422804]
                            },
                            "best_fit_line": {
                                "slope_s": 0.0,
                                "intercept_s": 0.416481,
                                "min_latency_s": 0.2915367,
                                "max_latency_s": 0.5414253,
                            },
                        }
                    },
                },
            },
            "instance_summary": {
                "simple_call-0_0_2-f1:entry_point:0": {
                    "invocations": 40,
                    "cpu_utilization": 0.05506742802346618,
                    "executions": {
                        "at_region": {
                            "aws:us-east-1": {
                                "durations_s": [5.07, 5.069, 5.069, 5.069, 5.07],
                                "auxiliary_data": {
                                    "5.07": [[3.828108310699463e-05, 0.068], [2.7456320822238922e-05, 0.068]]
                                },
                            }
                        }
                    },
                    "to_instance": {
                        "simple_call-0_0_2-f2:simple_call-0_0_2-f1_0_0:1": {
                            "invoked": 40,
                            "transfer_sizes_gb": [2.1345913410186768e-06] * 5,
                            "invocation_probability": 1.0,
                            "regions_to_regions": {
                                "aws:us-east-1": {
                                    "aws:us-east-1": {
                                        "transfer_size_gb_to_transfer_latencies_s": {
                                            "9.5367431640625e-06": [0.356507, 0.214345, 0.267174]
                                        },
                                        "best_fit_line": {
                                            "slope_s": 0.0,
                                            "intercept_s": 0.2809869,
                                            "min_latency_s": 0.19669082999999998,
                                            "max_latency_s": 0.36528297,
                                        },
                                    }
                                }
                            },
                        }
                    },
                }
            },
        }

        self.loader.set_workflow_data(self.workflow_data)

    @patch.object(WorkflowLoader, "_retrieve_workflow_data")
    def test_setup(self, mock_retrieve_workflow_data):
        mock_retrieve_workflow_data.return_value = self.workflow_data
        self.loader.setup("workflow_id")
        self.assertEqual(self.loader.get_workflow_data(), self.workflow_data)

    def test_get_home_region(self):
        self.assertEqual(self.loader.get_home_region(), "aws:ca-west-1")

    def test_get_workflow_placement_decision_size(self):
        size = self.loader.get_workflow_placement_decision_size()
        self.assertEqual(size, 1.7611309885978699e-06)

    def test_get_start_hop_retrieve_wpd_probability(self):
        probability = self.loader.get_start_hop_retrieve_wpd_probability()
        self.assertEqual(probability, 1.0)

    def test_get_start_hop_size_distribution(self):
        sizes = self.loader.get_start_hop_size_distribution()
        self.assertEqual(sizes, [1.9185245037078857e-07] * 5)

    def test_get_start_hop_best_fit_line(self):
        best_fit_line = self.loader.get_start_hop_best_fit_line("aws:us-east-1")
        self.assertEqual(best_fit_line["slope_s"], 0.0)

    def test_get_start_hop_latency_distribution(self):
        latency_distribution = self.loader.get_start_hop_latency_distribution("aws:us-east-1", 9.5367431640625e-06)
        self.assertEqual(latency_distribution, [0.42088, 0.41369, 0.422804])

    def test_get_average_cpu_utilization(self):
        cpu_utilization = self.loader.get_average_cpu_utilization(
            "simple_call-0_0_2-f1:entry_point:0", "aws:us-east-1", False
        )
        self.assertEqual(cpu_utilization, 0.05506742802346618)

    def test_get_runtime_distribution(self):
        runtime_distribution = self.loader.get_runtime_distribution(
            "simple_call-0_0_2-f1:entry_point:0", "aws:us-east-1", False
        )
        self.assertEqual(runtime_distribution, [5.07, 5.069, 5.069, 5.069, 5.07])

    def test_get_auxiliary_data_distribution(self):
        auxiliary_data_distribution = self.loader.get_auxiliary_data_distribution(
            "simple_call-0_0_2-f1:entry_point:0", "aws:us-east-1", 5.07, False
        )
        self.assertEqual(auxiliary_data_distribution, [[3.828108310699463e-05, 0.068], [2.7456320822238922e-05, 0.068]])

    def test_get_auxiliary_index_translation(self):
        translation = self.loader.get_auxiliary_index_translation("simple_call-0_0_2-f1:entry_point:0", False)
        self.assertEqual(translation, {})

    def test_get_invocation_probability(self):
        invocation_probability = self.loader.get_invocation_probability(
            "simple_call-0_0_2-f1:entry_point:0", "simple_call-0_0_2-f2:simple_call-0_0_2-f1_0_0:1"
        )
        self.assertEqual(invocation_probability, 1.0)

    def test_get_data_transfer_size_distribution(self):
        data_transfer_size_distribution = self.loader.get_data_transfer_size_distribution(
            "simple_call-0_0_2-f1:entry_point:0", "simple_call-0_0_2-f2:simple_call-0_0_2-f1_0_0:1"
        )
        self.assertEqual(data_transfer_size_distribution, [2.1345913410186768e-06] * 5)

    def test_get_latency_distribution_best_fit_line(self):
        best_fit_line = self.loader.get_latency_distribution_best_fit_line(
            "simple_call-0_0_2-f1:entry_point:0",
            "simple_call-0_0_2-f2:simple_call-0_0_2-f1_0_0:1",
            "aws:us-east-1",
            "aws:us-east-1",
        )
        self.assertEqual(best_fit_line["slope_s"], 0.0)

    def test_get_latency_distribution(self):
        latency_distribution = self.loader.get_latency_distribution(
            "simple_call-0_0_2-f1:entry_point:0",
            "simple_call-0_0_2-f2:simple_call-0_0_2-f1_0_0:1",
            "aws:us-east-1",
            "aws:us-east-1",
            9.5367431640625e-06,
        )
        self.assertEqual(latency_distribution, [0.356507, 0.214345, 0.267174])

    def test_get_non_execution_information(self):
        non_execution_info = self.loader.get_non_execution_information(
            "simple_call-0_0_2-f1:entry_point:0", "simple_call-0_0_2-f2:simple_call-0_0_2-f1_0_0:1"
        )
        self.assertEqual(non_execution_info, {})

    def test_get_non_execution_sns_transfer_size(self):
        sns_transfer_size = self.loader.get_non_execution_sns_transfer_size(
            "simple_call-0_0_2-f1:entry_point:0", "simple_call-0_0_2-f2:simple_call-0_0_2-f1_0_0:1", "sync"
        )
        self.assertEqual(
            sns_transfer_size, self.loader._round_to_kb(1e-09, 1)
        )  # assuming a small size for rounding test

    def test_get_non_execution_transfer_latency_distribution(self):
        latency_distribution = self.loader.get_non_execution_transfer_latency_distribution(
            "simple_call-0_0_2-f1:entry_point:0",
            "simple_call-0_0_2-f2:simple_call-0_0_2-f1_0_0:1",
            "sync",
            "aws:us-east-1",
            "aws:us-east-1",
        )
        self.assertEqual(latency_distribution, [])

    def test_get_sync_size(self):
        sync_size = self.loader.get_sync_size(
            "simple_call-0_0_2-f1:entry_point:0", "simple_call-0_0_2-f2:simple_call-0_0_2-f1_0_0:1"
        )
        self.assertEqual(sync_size, SYNC_SIZE_DEFAULT)

    def test_get_sns_only_size(self):
        sns_only_size = self.loader.get_sns_only_size(
            "simple_call-0_0_2-f1:entry_point:0", "simple_call-0_0_2-f2:simple_call-0_0_2-f1_0_0:1"
        )
        self.assertEqual(sns_only_size, SNS_SIZE_DEFAULT)

    def test_get_vcpu(self):
        vcpu = self.loader.get_vcpu("simple_call-0_0_2-f1:entry_point:0", "aws")
        self.assertEqual(vcpu, 2)

    def test_get_memory(self):
        memory = self.loader.get_memory("simple_call-0_0_2-f1:entry_point:0", "aws")
        self.assertEqual(memory, 2048)

    def test_get_architecture(self):
        architecture = self.loader.get_architecture("simple_call-0_0_2-f1:entry_point:0", "aws")
        self.assertEqual(architecture, "x86_64")

    def test_round_to_kb(self):
        rounded = self.loader._round_to_kb(9.765625e-07, 1, False)
        self.assertAlmostEqual(rounded, 9.53674316e-7)

    def test_round_to_ms(self):
        rounded = self.loader._round_to_ms(0.002, 10, True)
        self.assertEqual(rounded, 0.01)


if __name__ == "__main__":
    unittest.main()
