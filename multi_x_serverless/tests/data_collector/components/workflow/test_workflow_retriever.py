import json
import unittest
from unittest.mock import Mock, patch
from datetime import datetime, timedelta
from multi_x_serverless.data_collector.components.workflow.workflow_retriever import WorkflowRetriever
from multi_x_serverless.common.constants import TIME_FORMAT_DAYS, GLOBAL_TIME_ZONE


class TestWorkflowRetriever(unittest.TestCase):
    def setUp(self):
        self.mock_client = Mock()
        self.workflow_retriever = WorkflowRetriever(self.mock_client)
        self.maxDiff = None

    def test_retrieve_all_workflow_ids(self):
        # Set up the mock
        self.mock_client.get_keys.return_value = ["workflow1", "workflow2", "workflow3"]

        # Call the method
        result = self.workflow_retriever.retrieve_all_workflow_ids()

        # Check that the result is as expected
        expected_result = {"workflow1", "workflow2", "workflow3"}
        self.assertEqual(result, expected_result)

        # Check that get_keys was called with the correct argument
        self.mock_client.get_keys.assert_called_once_with(self.workflow_retriever._workflow_summary_table)

    def test_retrieve_workflow_summary(self):
        # Set up the mocks
        self.mock_client.get_value_from_table.return_value = "workflow_summary"
        self.workflow_retriever._transform_workflow_summary = Mock(return_value={"transformed": "workflow_summary"})

        # Call the method
        result = self.workflow_retriever.retrieve_workflow_summary("workflow_id")

        # Check that the result is as expected
        expected_result = {"transformed": "workflow_summary"}
        self.assertEqual(result, expected_result)

        # Check that get_value_from_table and _transform_workflow_summary were called with the correct arguments
        self.mock_client.get_value_from_table.assert_called_once_with(
            self.workflow_retriever._workflow_summary_table, "workflow_id"
        )
        self.workflow_retriever._transform_workflow_summary.assert_called_once_with("workflow_summary")

    def test_transform_workflow_summary(self):
        # Set up the mocks
        self.workflow_retriever._construct_summaries = Mock(return_value=("start_hop_summary", "instance_summary"))

        # Set up the test data
        workflow_summarized = json.dumps(
            {
                "workflow_runtime_samples": "runtime_samples",
                "daily_invocation_counts": "daily_counts",
                "logs": "logs",
            }
        )

        # Call the method
        result = self.workflow_retriever._transform_workflow_summary(workflow_summarized)

        # Check that the result is as expected
        expected_result = {
            "workflow_runtime_samples": "runtime_samples",
            "daily_invocation_counts": "daily_counts",
            "start_hop_summary": "start_hop_summary",
            "instance_summary": "instance_summary",
        }
        self.assertEqual(result, expected_result)

        self.workflow_retriever._construct_summaries.assert_called_once_with("logs")

    def test_construct_summaries(self):
        # Set up the mocks
        with patch.object(
            self.workflow_retriever, "_extend_start_hop_summary", autospec=True
        ) as mock_extend_start_hop_summary, patch.object(
            self.workflow_retriever, "_extend_instance_summary", autospec=True
        ) as mock_extend_instance_summary:
            # Set up the test data
            logs = [{"log": i} for i in range(5)]

            # Call the method
            start_hop_summary, instance_summary = self.workflow_retriever._construct_summaries(logs)

            # Check that the result is as expected
            self.assertEqual(start_hop_summary, {})
            self.assertEqual(instance_summary, {})

            # Check that _extend_start_hop_summary and _extend_instance_summary were called with the correct arguments
            for log in logs:
                mock_extend_start_hop_summary.assert_any_call(start_hop_summary, log)
                mock_extend_instance_summary.assert_any_call(instance_summary, log)

    def test_extend_start_hop_summary(self):
        # Set up the test data
        start_hop_summary = {}
        log = {
            "start_hop_destination": {"provider": "provider1", "region": "region1"},
            "start_hop_data_transfer_size": "1.0",
            "start_hop_latency": "0.1",
        }

        # Call the method
        self.workflow_retriever._extend_start_hop_summary(start_hop_summary, log)

        # Check that the start_hop_summary dictionary was updated as expected
        expected_result = {
            "provider1:region1": {
                1.0: ["0.1"],
            },
        }
        self.assertEqual(start_hop_summary, expected_result)

    def test_extend_instance_summary(self):
        # Set up the test data
        instance_summary = {}
        log = {
            "execution_latencies": {"instance1": {"provider_region": "provider1:region1", "latency": 0.1}},
            "start_hop_destination": {"provider": "provider1", "region": "region1"},
            "transmission_data": [
                {
                    "from_instance": "instance1",
                    "to_instance": "instance2",
                    "from_region": {"provider": "provider1", "region": "region1"},
                    "to_region": {"provider": "provider2", "region": "region2"},
                    "transmission_size": 1.0,
                    "transmission_latency": 0.1,
                }
            ],
            "non_executions": {"instance1": {"instance2": 1}},
        }

        self.workflow_retriever._handle_missing_region_to_region_transmission_data = Mock()
        # Call the method
        self.workflow_retriever._extend_instance_summary(instance_summary, log)

        # Check that the instance_summary dictionary was updated as expected
        expected_result = {
            "instance1": {
                "invocations": 1,
                "executions": {"provider1:region1": [0.1]},
                "to_instance": {
                    "instance2": {
                        "invoked": 0,
                        "regions_to_regions": {},
                        "non_executions": 1,
                        "invocation_probability": 0.0,
                    }
                },
            }
        }
        self.assertEqual(instance_summary, expected_result)

    def test_handle_missing_region_to_region_transmission_data_common_sample(self):
        # Set up the test data
        instance_summary = {
            "instance1": {
                "to_instance": {
                    "instance2": {
                        "regions_to_regions": {
                            "provider1:region1": {
                                "provider2:region2": {
                                    "transfer_size_to_transfer_latencies": {
                                        1.0: [0.1, 0.2, 0.3],
                                        2.0: [0.2, 0.3, 0.4],
                                    },
                                    "transfer_sizes": [1.0, 1.0, 1.0, 2.0, 2.0, 2.0],
                                },
                                "provider2:region3": {
                                    "transfer_size_to_transfer_latencies": {
                                        1.0: [0.1, 0.2, 0.3],
                                    },
                                    "transfer_sizes": [1.0, 1.0, 1.0],
                                },
                            }
                        }
                    }
                }
            }
        }

        # Call the method
        self.workflow_retriever._handle_missing_region_to_region_transmission_data(instance_summary)

        # Check that the instance_summary dictionary was updated as expected
        # The expected_result will depend on the specific behavior of your method
        expected_result = {
            "instance1": {
                "to_instance": {
                    "instance2": {
                        "regions_to_regions": {
                            "provider1:region1": {
                                "provider2:region2": {
                                    "transfer_size_to_transfer_latencies": {1.0: [0.1, 0.2, 0.3], 2.0: [0.2, 0.3, 0.4]},
                                    "transfer_sizes": [1.0, 1.0, 1.0, 2.0, 2.0, 2.0],
                                },
                                "provider2:region3": {
                                    "transfer_size_to_transfer_latencies": {
                                        1.0: [0.1, 0.2, 0.3],
                                        2.0: [0.15000000000000002, 0.30000000000000004, 0.44999999999999996],
                                    },
                                    "transfer_sizes": [1.0, 1.0, 1.0, 2.0, 2.0, 2.0],
                                },
                            }
                        }
                    }
                }
            }
        }
        self.assertEqual(instance_summary, expected_result)

    def test_handle_missing_region_to_region_transmission_data_no_common_sample(self):
        # Set up the test data
        instance_summary = {
            "instance1": {
                "to_instance": {
                    "instance2": {
                        "regions_to_regions": {
                            "provider1:region1": {
                                "provider2:region2": {
                                    "transfer_size_to_transfer_latencies": {
                                        2.0: [0.2, 0.3, 0.4],
                                    },
                                    "transfer_sizes": [2.0, 2.0, 2.0],
                                },
                                "provider2:region3": {
                                    "transfer_size_to_transfer_latencies": {
                                        1.0: [0.1, 0.2, 0.3],
                                    },
                                    "transfer_sizes": [1.0, 1.0, 1.0],
                                },
                            }
                        }
                    }
                }
            }
        }

        # Call the method
        self.workflow_retriever._handle_missing_region_to_region_transmission_data(instance_summary)

        # Check that the instance_summary dictionary was updated as expected
        # The expected_result will depend on the specific behavior of your method
        expected_result = {
            "instance1": {
                "to_instance": {
                    "instance2": {
                        "regions_to_regions": {
                            "provider1:region1": {
                                "provider2:region2": {
                                    "transfer_size_to_transfer_latencies": {
                                        2.0: [0.2, 0.3, 0.4],
                                        1.0: [0.13333333333333333, 0.19999999999999998, 0.26666666666666666],
                                    },
                                    "transfer_sizes": [2.0, 2.0, 2.0, 1.0, 1.0, 1.0],
                                },
                                "provider2:region3": {
                                    "transfer_size_to_transfer_latencies": {
                                        1.0: [0.1, 0.2, 0.3],
                                        2.0: [0.15000000000000002, 0.30000000000000004, 0.44999999999999996],
                                    },
                                    "transfer_sizes": [1.0, 1.0, 1.0, 2.0, 2.0, 2.0],
                                },
                            }
                        }
                    }
                }
            }
        }
        self.assertEqual(instance_summary, expected_result)


if __name__ == "__main__":
    unittest.main()
