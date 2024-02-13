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

    def test_get_favourite_home_region(self):
        filtered_execution_summary = {
            "aws:region1": {"invocation_count": 1, "total_runtime": 1, "total_tail_runtime": 1},
            "aws:region2": {"invocation_count": 2, "total_runtime": 2, "total_tail_runtime": 2},
        }
        result = self.workflow_retriever.get_favourite_home_region(filtered_execution_summary)
        self.assertEqual(result, "aws:region2")

    def test_retrieve_workflow_summary(self):
        self.mock_client.get_all_values_from_sort_key_table.return_value = [
            {
                "sort_key": "2021-2-10T10:10:10",
                "value": '{"time_since_last_sync": 30, "instance_summary": {"instance1": {"invocation_count": 1, "execution_summary": {"aws:region1": {"invocation_count": 1, "average_runtime": 1, "tail_runtime": 1}}}}}',
            }
        ]
        self.workflow_retriever._available_regions = {"aws:region1": {}}

        result = self.workflow_retriever.retrieve_workflow_summary("id1")
        expected_result = {
            "instance1": {
                "favourite_home_region": "aws:region1",
                "favourite_home_region_average_runtime": 1.0,
                "favourite_home_region_tail_runtime": 1.0,
                "projected_monthly_invocations": 1.0,
                "execution_summary": {"aws:region1": {"average_runtime": 1.0, "tail_runtime": 1.0, "unit": "s"}},
                "invocation_summary": {},
            }
        }
        self.assertEqual(result, expected_result)

    def test_consolidate_logs(self):
        logs = [
            {
                "sort_key": "2021-2-10T10:10:10",
                "value": json.dumps(
                    {
                        "time_since_last_sync": 240,
                        "instance_summary": {
                            "instance_1": {
                                "invocation_count": 100,
                                "execution_summary": {
                                    "provider_1:region_1": {
                                        "invocation_count": 80,
                                        "average_runtime": 25,  # In s
                                        "tail_runtime": 30,  # In s
                                    },
                                    "provider_1:region_2": {
                                        "invocation_count": 20,
                                        "average_runtime": 30,  # In s
                                        "tail_runtime": 35,  # In s
                                    },
                                },
                                "invocation_summary": {
                                    "instance_2": {
                                        "invocation_count": 80,
                                        "average_data_transfer_size": 0.0007,  # In GB
                                        "transmission_summary": {
                                            "provider_1:region_1": {
                                                "provider_1:region_1": {
                                                    "transmission_count": 65,
                                                    "average_latency": 0.001,  # In s
                                                    "tail_latency": 0.002,  # In s
                                                },
                                                "provider_1:region_2": {
                                                    "transmission_count": 5,
                                                    "average_latency": 0.12,  # In s
                                                    "tail_latency": 0.15,  # In s
                                                },
                                            },
                                            "provider_1:region_2": {
                                                "provider_1:region_1": {
                                                    "transmission_count": 10,
                                                    "average_latency": 0.1,  # In s
                                                    "tail_latency": 0.12,  # In s
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
                                        "average_runtime": 10,  # In s
                                        "tail_runtime": 15,  # In s
                                    },
                                    "provider_1:region_2": {
                                        "invocation_count": 10,
                                        "average_runtime": 15,  # In s
                                        "tail_runtime": 10,  # In s
                                    },
                                },
                            },
                        },
                    }
                ),
            },
            {
                "sort_key": "2021-3-10T10:10:20",
                "value": json.dumps(
                    {
                        "time_since_last_sync": 240,
                        "instance_summary": {
                            "instance_1": {
                                "invocation_count": 100,
                                "execution_summary": {
                                    "provider_1:region_1": {
                                        "invocation_count": 20,
                                        "average_runtime": 30,  # In s
                                        "tail_runtime": 35,  # In s
                                    },
                                    "provider_1:region_2": {
                                        "invocation_count": 80,
                                        "average_runtime": 25,  # In s
                                        "tail_runtime": 30,  # In s
                                    },
                                },
                                "invocation_summary": {
                                    "instance_2": {
                                        "invocation_count": 80,
                                        "average_data_transfer_size": 0.0007,  # In GB
                                        "transmission_summary": {
                                            "provider_1:region_1": {
                                                "provider_1:region_1": {
                                                    "transmission_count": 65,
                                                    "average_latency": 0.0015,  # In s
                                                    "tail_latency": 0.0015,  # In s
                                                },
                                                "provider_1:region_2": {
                                                    "transmission_count": 5,
                                                    "average_latency": 0.13,  # In s
                                                    "tail_latency": 0.16,  # In s
                                                },
                                            },
                                            "provider_1:region_2": {
                                                "provider_1:region_1": {
                                                    "transmission_count": 10,
                                                    "average_latency": 0.09,  # In s
                                                    "tail_latency": 0.13,  # In s
                                                }
                                            },
                                        },
                                    }
                                },
                            },
                            "instance_2": {
                                "invocation_count": 80,
                                "execution_summary": {
                                    "provider_1:region_1": {
                                        "invocation_count": 70,
                                        "average_runtime": 15,  # In s
                                        "tail_runtime": 10,  # In s
                                    },
                                    "provider_1:region_2": {
                                        "invocation_count": 10,
                                        "average_runtime": 10,  # In s
                                        "tail_runtime": 15,  # In s
                                    },
                                },
                            },
                        },
                    }
                ),
            },
        ]

        self.workflow_retriever._available_regions = {"provider_1:region_1": {}, "provider_1:region_2": {}}

        result = self.workflow_retriever._consolidate_logs(logs=logs)

        expected_result = {
            "instance_1": {
                "favourite_home_region": "provider_1:region_1",
                "favourite_home_region_average_runtime": 26.0,
                "favourite_home_region_tail_runtime": 31.0,
                "projected_monthly_invocations": 12.5,
                "execution_summary": {
                    "provider_1:region_1": {"average_runtime": 26.0, "tail_runtime": 31.0, "unit": "s"},
                    "provider_1:region_2": {"average_runtime": 26.0, "tail_runtime": 31.0, "unit": "s"},
                },
                "invocation_summary": {
                    "instance_2": {
                        "probability_of_invocation": 0.8,
                        "average_data_transfer_size": 0.0007,
                        "transmission_summary": {
                            "provider_1:region_1": {
                                "provider_1:region_1": {
                                    "average_latency": 0.00125,
                                    "tail_latency": 0.00175,
                                    "unit": "s",
                                },
                                "provider_1:region_2": {"average_latency": 0.125, "tail_latency": 0.155, "unit": "s"},
                            },
                            "provider_1:region_2": {
                                "provider_1:region_1": {"average_latency": 0.095, "tail_latency": 0.125, "unit": "s"},
                            },
                        },
                    },
                },
            },
            "instance_2": {
                "favourite_home_region": "provider_1:region_1",
                "favourite_home_region_average_runtime": 12.5,
                "favourite_home_region_tail_runtime": 12.5,
                "projected_monthly_invocations": 11.25,
                "execution_summary": {
                    "provider_1:region_1": {"average_runtime": 12.5, "tail_runtime": 12.5, "unit": "s"},
                    "provider_1:region_2": {"average_runtime": 12.5, "tail_runtime": 12.5, "unit": "s"},
                },
                "invocation_summary": {},
            },
        }

        self.assertEqual(result, expected_result)


if __name__ == "__main__":
    unittest.main()
