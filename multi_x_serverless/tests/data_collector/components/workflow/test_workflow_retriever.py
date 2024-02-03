import unittest
from unittest.mock import Mock
from multi_x_serverless.data_collector.components.workflow.workflow_retriever import WorkflowRetriever


class TestWorkflowRetriever(unittest.TestCase):
    def setUp(self):
        self.mock_client = Mock()
        self.workflow_retriever = WorkflowRetriever(self.mock_client)

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
                "value": '{"months_between_summary": 1, "instance_summary": {"instance1": {"invocation_count": 1, "execution_summary": {"aws:region1": {"invocation_count": 1, "average_runtime": 1, "tail_runtime": 1}}}}}'
            }
        ]
        self.workflow_retriever._available_regions = {"aws:region1": {"data": "data1"}}

        result = self.workflow_retriever.retrieve_workflow_summary("id1")
        expected_result = {
            "instance1": {
                "favourite_home_region": "aws:region1",
                "favourite_home_region_average_runtime": 1.0,
                "favourite_home_region_tail_runtime": 1.0,
                "projected_monthly_invocations": 1.0,
                "execution_summary": {"aws:region1": {"average_runtime": 1.0, "tail_runtime": 1.0}},
                "invocation_summary": {},
            }
        }
        self.assertEqual(result, expected_result)

    def test_consolidate_logs(self):
        # TODO: Implement this test
        log = {
            "2021-2-10T10:10:10": {
                "months_between_summary": 8,
                "instance_summary": {
                    "instance_1": {
                        "invocation_count": 100,
                        "execution_summary": {
                            "provider_1:region_1": {
                                "invocation_count": 90,
                                "average_runtime": 20,  # In s
                                "tail_runtime": 30,  # In s
                            },
                            "provider_1:region_2": {
                                "invocation_count": 10,
                                "average_runtime": 17,  # In s
                                "tail_runtime": 25,  # In s
                            },
                        },
                        "invocation_summary": {
                            "instance_2": {
                                "invocation_count": 80,
                                "average_data_transfer_size": 0.0007,  # In GB
                                "transmission_summary": {
                                    "provider_1:region_1": {
                                        "provider_1:region_1": {
                                            "transmission_count": 50,
                                            "average_latency": 0.001,  # In s
                                            "tail_latency": 0.002,  # In s
                                        },
                                        "provider_1:region_2": {
                                            "transmission_count": 22,
                                            "average_latency": 0.12,  # In s
                                            "tail_latency": 0.15,  # In s
                                        },
                                    },
                                    "provider_1:region_2": {
                                        "provider_1:region_1": {
                                            "transmission_count": 8,
                                            "average_latency": 0.1,  # In s
                                            "tail_latency": 0.12,  # In s
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
                                "invocation_count": 58,
                                "average_runtime": 10,  # In s
                                "tail_runtime": 15,  # In s
                            },
                            "provider_1:region_2": {
                                "invocation_count": 22,
                                "average_runtime": 12,  # In s
                                "tail_runtime": 17,  # In s
                            },
                        },
                    },
                },
            },
            "2021-10-10T10:10:20": {
                "months_between_summary": 8,
                "instance_summary": {
                    "instance_1": {
                        "invocation_count": 200,
                        "execution_summary": {
                            "provider_1:region_1": {
                                "invocation_count": 144,
                                "average_runtime": 22,  # In s
                                "tail_runtime": 33,  # In s
                            },
                            "provider_1:region_2": {
                                "invocation_count": 56,
                                "average_runtime": 15,  # In s
                                "tail_runtime": 27,  # In s
                            },
                        },
                        "invocation_summary": {
                            "instance_2": {
                                "invocation_count": 160,
                                "average_data_transfer_size": 0.0007,  # In GB
                                "transmission_summary": {
                                    "provider_1:region_1": {
                                        "provider_1:region_1": {
                                            "transmission_count": 100,
                                            "average_latency": 0.0012,  # In s
                                            "tail_latency": 0.0021,  # In s
                                        },
                                        "provider_1:region_2": {
                                            "transmission_count": 44,
                                            "average_latency": 0.13,  # In s
                                            "tail_latency": 0.16,  # In s
                                        },
                                    },
                                    "provider_1:region_2": {
                                        "provider_1:region_1": {
                                            "transmission_count": 16,
                                            "average_latency": 0.09,  # In s
                                            "tail_latency": 0.13,  # In s
                                        }
                                    },
                                },
                            }
                        },
                    },
                    "instance_2": {
                        "invocation_count": 160,
                        "execution_summary": {
                            "provider_1:region_1": {
                                "invocation_count": 116,
                                "average_runtime": 12,  # In s
                                "tail_runtime": 16,  # In s
                            },
                            "provider_1:region_2": {
                                "invocation_count": 44,
                                "average_runtime": 11,  # In s
                                "tail_runtime": 16,  # In s
                            },
                        },
                    },
                },
            },
        }

        return


if __name__ == "__main__":
    unittest.main()
