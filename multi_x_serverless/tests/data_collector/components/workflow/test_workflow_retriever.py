import json
import unittest
from unittest.mock import Mock
from multi_x_serverless.data_collector.components.workflow.workflow_retriever import WorkflowRetriever


class TestWorkflowRetriever(unittest.TestCase):
    def setUp(self):
        self.mock_client = Mock()
        self.workflow_retriever = WorkflowRetriever(self.mock_client)
        self.maxDiff = None

    def test_retrieve_all_workflow_ids(self):
        self.mock_client.get_all_values_from_table.return_value = {"id1": "value1", "id2": "value2"}
        result = self.workflow_retriever.retrieve_all_workflow_ids()
        self.assertEqual(result, {"id1", "id2"})

    def test_retrieve_workflow_summary(self):
        self.mock_client.get_value_from_table.return_value = '{"time_since_last_sync": 30, "total_invocations": 10, "instance_summary": {"instance1": {"invocation_count": 1, "execution_summary": {"aws:region1": {"invocation_count": 1, "runtime_samples": [1, 1, 1, 1, 1, 1, 1, 1, 1, 1]}}}}}'

        self.workflow_retriever._available_regions = {"aws:region1": {}}

        result = self.workflow_retriever.retrieve_workflow_summary("id1")
        expected_result = {
            "total_invocations": 10,
            "instance1": {
                "projected_monthly_invocations": 1.0,
                "execution_summary": {
                    "aws:region1": {
                        "runtime_samples": [0.001, 0.001, 0.001, 0.001, 0.001, 0.001, 0.001, 0.001, 0.001, 0.001],
                        "invocation_count": 1,
                        "unit": "s",
                    }
                },
                "invocation_summary": {},
            },
        }
        self.assertEqual(result, expected_result)

    def test_consolidate_log(self):
        log = json.dumps(
            {
                "time_since_last_sync": 240,
                "total_invocations": 200,
                "instance_summary": {
                    "instance_1": {
                        "invocation_count": 100,
                        "execution_summary": {
                            "provider_1:region_1": {
                                "init_data_transfer_size_samples": [2],  # In GB
                                "init_latency_samples": [500],  # In ms
                                "invocation_count": 80,
                                "runtime_samples": [25],  # In ms
                            },
                            "provider_1:region_2": {
                                "init_data_transfer_size_samples": [6],  # In GB
                                "init_latency_samples": [300],  # In ms
                                "invocation_count": 20,
                                "runtime_samples": [30, 30, 30],  # In ms
                            },
                        },
                        "invocation_summary": {
                            "instance_2": {
                                "invocation_count": 80,
                                "data_transfer_samples": [1, 11],  # In GB
                                "transmission_summary": {
                                    "provider_1:region_1": {
                                        "provider_1:region_1": {
                                            "transmission_count": 65,
                                            "latency_samples": [
                                                200,
                                                200,
                                            ],  # In ms
                                        },
                                        "provider_1:region_2": {
                                            "transmission_count": 5,
                                            "latency_samples": [
                                                150,
                                                150,
                                            ],  # In ms
                                        },
                                    },
                                    "provider_1:region_2": {
                                        "provider_1:region_1": {
                                            "transmission_count": 10,
                                            "latency_samples": [
                                                100,
                                                100,
                                            ],  # In ms
                                        }
                                    },
                                },
                            }
                        },
                    },
                    "instance_2": {
                        "invocation_count": 100,
                        "execution_summary": {
                            "provider_1:region_1": {
                                "invocation_count": 70,
                                "runtime_samples": [10, 10],  # In ms
                            },
                            "provider_1:region_2": {
                                "invocation_count": 10,
                                "runtime_samples": [15, 15, 15],  # In ms
                            },
                        },
                    },
                },
            }
        )

        self.workflow_retriever._available_regions = {"provider_1:region_1": {}, "provider_1:region_2": {}}

        result = self.workflow_retriever._consolidate_log(log=log)

        expected_result = {
            "total_invocations": 200,
            "instance_1": {
                "projected_monthly_invocations": 100.0,
                "execution_summary": {
                    "provider_1:region_1": {
                        "runtime_samples": [0.025],
                        "invocation_count": 80,
                        "unit": "s",
                        "init_data_transfer_size_samples": [2],
                        "init_latency_samples": [0.5],
                    },
                    "provider_1:region_2": {
                        "runtime_samples": [0.03, 0.03, 0.03],
                        "invocation_count": 20,
                        "unit": "s",
                        "init_data_transfer_size_samples": [6],
                        "init_latency_samples": [0.3],
                    },
                },
                "invocation_summary": {
                    "instance_2": {
                        "probability_of_invocation": 0.8,
                        "data_transfer_samples": [1, 11],
                        "transmission_summary": {
                            "provider_1:region_1": {
                                "provider_1:region_1": {"latency_samples": [0.2, 0.2], "unit": "s"},
                                "provider_1:region_2": {"latency_samples": [0.15, 0.15], "unit": "s"},
                            },
                            "provider_1:region_2": {
                                "provider_1:region_1": {"latency_samples": [0.1, 0.1], "unit": "s"}
                            },
                        },
                    }
                },
            },
            "instance_2": {
                "projected_monthly_invocations": 100.0,
                "execution_summary": {
                    "provider_1:region_1": {"runtime_samples": [0.01, 0.01], "invocation_count": 70, "unit": "s"},
                    "provider_1:region_2": {
                        "runtime_samples": [0.015, 0.015, 0.015],
                        "invocation_count": 10,
                        "unit": "s",
                    },
                },
                "invocation_summary": {},
            },
        }
        self.assertEqual(result, expected_result)


if __name__ == "__main__":
    unittest.main()
