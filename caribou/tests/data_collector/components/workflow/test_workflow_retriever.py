import json
import unittest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timedelta
from caribou.data_collector.components.workflow.workflow_retriever import WorkflowRetriever
from caribou.common.constants import TIME_FORMAT_DAYS, GLOBAL_TIME_ZONE, WORKFLOW_SUMMARY_TABLE


class TestWorkflowRetriever(unittest.TestCase):
    def setUp(self):
        self.mock_client = Mock()
        self.workflow_retriever = WorkflowRetriever(self.mock_client)
        self.maxDiff = None

        self.sample_workflow_summarized = json.dumps(
            {
                "logs": [
                    {
                        "runtime_s": 100,
                        "start_hop_info": {
                            "destination": "region-1",
                            "data_transfer_size_gb": 0.5,
                            "latency_s": 10,
                            "workflow_placement_decision": {"data_size_gb": 1.0},
                        },
                        "execution_data": [
                            {
                                "instance_name": "instance-1",
                                "provider_region": "region-1",
                                "cpu_utilization": 0.8,
                                "duration_s": 50,
                                "data_transfer_during_execution_gb": 0.2,
                                "successor_data": {"successor-1": {"invocation_time_from_function_start_s": 5}},
                            }
                        ],
                        "transmission_data": [],
                    }
                ],
                "daily_invocation_counts": {"2024-06-15": 10},
                "daily_failure_counts": {"2024-06-15": 2},
            }
        )

    def test_initialization(self):
        self.assertEqual(self.workflow_retriever._workflow_summary_table, WORKFLOW_SUMMARY_TABLE)

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
        self.mock_client.get_value_from_table.return_value = ("workflow_summary", 0.0)
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
        self.workflow_retriever._construct_summaries = Mock(
            return_value=("start_hop_summary", "instance_summary", [100])
        )

        # Set up the test data
        workflow_summarized = json.dumps(
            {
                "logs": [
                    {
                        "runtime_s": 100,
                        "start_hop_info": {
                            "destination": "region-1",
                            "data_transfer_size_gb": 0.5,
                            "latency_s": 10,
                            "workflow_placement_decision": {"data_size_gb": 1.0},
                        },
                        "execution_data": [
                            {
                                "instance_name": "instance-1",
                                "provider_region": "region-1",
                                "cpu_utilization": 0.8,
                                "duration_s": 50,
                                "data_transfer_during_execution_gb": 0.2,
                                "successor_data": {"successor-1": {"invocation_time_from_function_start_s": 5}},
                            }
                        ],
                    }
                ],
                "daily_invocation_counts": {"2024-06-15": 10},
                "daily_failure_counts": {"2024-06-15": 2},
            }
        )

        # Call the method
        result = self.workflow_retriever._transform_workflow_summary(workflow_summarized)

        # Check that the result is as expected
        expected_result = {
            "workflow_runtime_samples": [100],
            "daily_invocation_counts": {"2024-06-15": 10},
            "daily_failure_counts": {"2024-06-15": 2},
            "start_hop_summary": "start_hop_summary",
            "instance_summary": "instance_summary",
        }
        self.assertEqual(result, expected_result)

        self.workflow_retriever._construct_summaries.assert_called_once_with(
            json.loads(workflow_summarized).get("logs", {})
        )

    def test_construct_summaries(self):
        # Set up the mocks
        with patch.object(
            self.workflow_retriever, "_extend_start_hop_summary", autospec=True
        ) as mock_extend_start_hop_summary, patch.object(
            self.workflow_retriever, "_extend_instance_summary", autospec=True
        ) as mock_extend_instance_summary, patch.object(
            self.workflow_retriever, "_reorganize_start_hop_summary", autospec=True
        ) as mock_reorganize_start_hop_summary, patch.object(
            self.workflow_retriever, "_reorganize_instance_summary", autospec=True
        ) as mock_reorganize_instance_summary:
            # Set up the test data
            logs = [{"log": i} for i in range(5)]

            # Call the method
            start_hop_summary, instance_summary, runtime_samples = self.workflow_retriever._construct_summaries(logs)

            # Check that the result is as expected
            self.assertEqual(
                start_hop_summary,
                {"workflow_placement_decision_size_gb": [], "transfer_size_gb_to_transfer_latencies_s": {}},
            )
            self.assertEqual(instance_summary, {})
            self.assertEqual(runtime_samples, [])

            # Check that _extend_start_hop_summary and _extend_instance_summary were called with the correct arguments
            for log in logs:
                mock_extend_start_hop_summary.assert_any_call(start_hop_summary, log)
                mock_extend_instance_summary.assert_any_call(instance_summary, log)

            mock_reorganize_start_hop_summary.assert_called_once()
            mock_reorganize_instance_summary.assert_called_once()

    def test_extend_start_hop_summary(self):
        # Set up the test data
        start_hop_summary = {"workflow_placement_decision_size_gb": [], "transfer_size_gb_to_transfer_latencies_s": {}}
        log = json.loads(self.sample_workflow_summarized)["logs"][0]

        self.workflow_retriever._extend_start_hop_summary(start_hop_summary, log)

        expected_start_hop_summary = {
            "workflow_placement_decision_size_gb": [1.0],
            "transfer_size_gb_to_transfer_latencies_s": {"region-1": {0.5: [10.0]}},
        }

        self.assertEqual(start_hop_summary, expected_start_hop_summary)

    def test_extend_instance_summary(self):
        instance_summary = {}
        log = json.loads(self.sample_workflow_summarized)["logs"][0]

        self.workflow_retriever._extend_instance_summary(instance_summary, log)

        expected_instance_summary = {
            "instance-1": {
                "invocations": 1,
                "cpu_utilization": [0.8],
                "executions": {
                    "at_region": {
                        "region-1": [
                            {
                                "duration_s": 50,
                                "data_transfer_during_execution_gb": 0.2,
                                "successor_invocations": {"successor-1": {"invocation_time_from_function_start_s": 5}},
                            }
                        ]
                    },
                    "successor_instances": {"successor-1"},
                },
            }
        }

        self.assertEqual(instance_summary, expected_instance_summary)

    def test_reorganize_start_hop_summary(self):
        start_hop_summary = {
            "workflow_placement_decision_size_gb": [1.0, 1.5],
            "transfer_size_gb_to_transfer_latencies_s": {"region-1": {0.00048828125: [10.0]}, "region-2": {}},
        }

        self.workflow_retriever._reorganize_start_hop_summary(start_hop_summary)

        expected_start_hop_summary = {
            "workflow_placement_decision_size_gb": 1.25,
            "transfer_size_gb_to_transfer_latencies_s": {"region-1": {0.00048828125: [10.0]}, "region-2": {}},
        }

        self.assertEqual(start_hop_summary, expected_start_hop_summary)

    def test_reorganize_instance_summary(self):
        instance_summary = {
            "instance-1": {
                "invocations": 1,
                "cpu_utilization": [0.8],
                "executions": {
                    "at_region": {
                        "region-1": [
                            {
                                "duration_s": 50,
                                "data_transfer_during_execution_gb": 0.2,
                                "successor_invocations": {"successor-1": {"invocation_time_from_function_start_s": 5}},
                            }
                        ]
                    },
                    "successor_instances": {"successor-1"},
                },
            }
        }

        self.workflow_retriever._reorganize_instance_summary(instance_summary)

        expected_instance_summary = {
            "instance-1": {
                "invocations": 1,
                "cpu_utilization": 0.8,
                "executions": {
                    "at_region": {"region-1": {"durations_s": [50], "auxiliary_data": {50: [[0.2, 5]]}}},
                    "auxiliary_index_translation": {"data_transfer_during_execution_gb": 0, "successor-1": 1},
                },
            }
        }

        self.assertEqual(instance_summary, expected_instance_summary)

    def test_handle_missing_region_to_region_transmission_data_common_sample(self):
        # Set up the test data
        instance_summary = {
            "instance1": {
                "to_instance": {
                    "instance2": {
                        "regions_to_regions": {
                            "provider1:region1": {
                                "provider2:region2": {
                                    "transfer_size_gb_to_transfer_latencies_s": {
                                        1.0: [0.1, 0.2, 0.3],
                                        2.0: [0.2, 0.3, 0.4],
                                    },
                                    "transfer_sizes_gb": [1.0, 1.0, 1.0, 2.0, 2.0, 2.0],
                                },
                                "provider2:region3": {
                                    "transfer_size_gb_to_transfer_latencies_s": {
                                        1.0: [0.1, 0.2, 0.3],
                                    },
                                    "transfer_sizes_gb": [1.0, 1.0, 1.0],
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
        expected_result = {
            "instance1": {
                "to_instance": {
                    "instance2": {
                        "regions_to_regions": {
                            "provider1:region1": {
                                "provider2:region2": {
                                    "transfer_size_gb_to_transfer_latencies_s": {
                                        1.0: [0.1, 0.2, 0.3],
                                        2.0: [0.2, 0.3, 0.4],
                                    },
                                    "transfer_sizes_gb": [1.0, 1.0, 1.0, 2.0, 2.0, 2.0],
                                    "best_fit_line": {
                                        "slope_s": 0.1,
                                        "intercept_s": 0.09999999999999998,
                                        "min_latency_s": 0.175,
                                        "max_latency_s": 0.325,
                                    },
                                },
                                "provider2:region3": {
                                    "transfer_size_gb_to_transfer_latencies_s": {1.0: [0.1, 0.2, 0.3]},
                                    "transfer_sizes_gb": [1.0, 1.0, 1.0],
                                    "best_fit_line": {
                                        "slope_s": 0.0,
                                        "intercept_s": 0.19999999999999998,
                                        "min_latency_s": 0.13999999999999999,
                                        "max_latency_s": 0.26,
                                    },
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
                                    "transfer_size_gb_to_transfer_latencies_s": {
                                        2.0: [0.2, 0.3, 0.4],
                                    },
                                    "transfer_sizes_gb": [2.0, 2.0, 2.0],
                                },
                                "provider2:region3": {
                                    "transfer_size_gb_to_transfer_latencies_s": {
                                        1.0: [0.1, 0.2, 0.3],
                                    },
                                    "transfer_sizes_gb": [1.0, 1.0, 1.0],
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
        expected_result = {
            "instance1": {
                "to_instance": {
                    "instance2": {
                        "regions_to_regions": {
                            "provider1:region1": {
                                "provider2:region2": {
                                    "transfer_size_gb_to_transfer_latencies_s": {2.0: [0.2, 0.3, 0.4]},
                                    "transfer_sizes_gb": [2.0, 2.0, 2.0],
                                    "best_fit_line": {
                                        "slope_s": 0.0,
                                        "intercept_s": 0.3,
                                        "min_latency_s": 0.21,
                                        "max_latency_s": 0.39,
                                    },
                                },
                                "provider2:region3": {
                                    "transfer_size_gb_to_transfer_latencies_s": {1.0: [0.1, 0.2, 0.3]},
                                    "transfer_sizes_gb": [1.0, 1.0, 1.0],
                                    "best_fit_line": {
                                        "slope_s": 0.0,
                                        "intercept_s": 0.19999999999999998,
                                        "min_latency_s": 0.13999999999999999,
                                        "max_latency_s": 0.26,
                                    },
                                },
                            }
                        }
                    }
                }
            }
        }
        self.assertEqual(instance_summary, expected_result)

    def test_round_to_kb(self):
        rounded_value = self.workflow_retriever._round_to_kb(0.5, 1)
        self.assertEqual(rounded_value, 0.5)

    def test_round_to_ms(self):
        rounded_value = self.workflow_retriever._round_to_ms(0.1234, 10)
        self.assertEqual(rounded_value, 0.13)


if __name__ == "__main__":
    unittest.main()
