import unittest
from unittest.mock import Mock, patch, MagicMock
import json
from typing import Any

# Import the WorkflowRetriever class
from caribou.common.constants import WORKFLOW_SUMMARY_TABLE
from caribou.data_collector.components.workflow.workflow_retriever import WorkflowRetriever


class TestWorkflowRetriever(unittest.TestCase):
    def setUp(self):
        # Initialize the WorkflowRetriever with a mocked client
        self.mock_client = Mock()
        self.workflow_retriever = WorkflowRetriever(self.mock_client)

    def test_retrieve_all_workflow_ids(self):
        # Mock the response from the client's get_keys method
        self.mock_client.get_keys.return_value = ["workflow1", "workflow2", "workflow3"]

        # Call the method
        result = self.workflow_retriever.retrieve_all_workflow_ids()

        # Assertions
        self.assertEqual(result, {"workflow1", "workflow2", "workflow3"})
        self.mock_client.get_keys.assert_called_once_with(WORKFLOW_SUMMARY_TABLE)

    def test_retrieve_workflow_summary(self):
        # Mock the response from the client's get_value_from_table method
        self.mock_client.get_value_from_table.return_value = (json.dumps({"logs": []}), 0.0)

        with patch.object(
            self.workflow_retriever, "_transform_workflow_summary", return_value={"transformed": "data"}
        ) as mock_transform:
            result = self.workflow_retriever.retrieve_workflow_summary("workflow_id")

            # Assertions
            self.assertEqual(result, {"transformed": "data"})
            self.mock_client.get_value_from_table.assert_called_once_with(WORKFLOW_SUMMARY_TABLE, "workflow_id")
            mock_transform.assert_called_once_with(json.dumps({"logs": []}))

    @patch.object(WorkflowRetriever, "_reorganize_instance_summary")
    @patch.object(WorkflowRetriever, "_reorganize_start_hop_summary")
    @patch.object(WorkflowRetriever, "_extend_instance_summary")
    @patch.object(WorkflowRetriever, "_extend_start_hop_summary")
    def test_construct_summaries(
        self, mock_extend_start_hop, mock_extend_instance, mock_reorganize_start_hop, mock_reorganize_instance
    ):
        # Create some sample logs (structure isn't important due to mocking)
        logs = [MagicMock(spec=dict) for _ in range(3)]

        # Run the method
        start_hop_summary, instance_summary, runtime_samples = self.workflow_retriever._construct_summaries(logs)

        # Assertions to check that the internal methods are called correctly
        self.assertEqual(mock_extend_start_hop.call_count, len(logs))
        self.assertEqual(mock_extend_instance.call_count, len(logs))
        self.assertTrue(mock_reorganize_start_hop.called)
        self.assertTrue(mock_reorganize_instance.called)

        # Check if the return values are as expected
        self.assertEqual(runtime_samples, [log.get("runtime_s", None) for log in logs])
        self.assertIsInstance(start_hop_summary, dict)
        self.assertIsInstance(instance_summary, dict)

        # Ensure the start_hop_summary dictionary is being updated
        for log in logs:
            mock_extend_start_hop.assert_any_call(start_hop_summary, log)

        # Ensure the instance_summary dictionary is being updated
        for log in logs:
            mock_extend_instance.assert_any_call(instance_summary, log)

    def test_transform_workflow_summary_empty(self):
        # Test when workflow_summarized is an empty string
        result = self.workflow_retriever._transform_workflow_summary("")
        self.assertEqual(result, {})

    @patch.object(WorkflowRetriever, "_construct_summaries", return_value=({}, {}, []))
    def test_transform_workflow_summary(self, mock_construct_summaries):
        # Test when workflow_summarized has data
        workflow_summarized = json.dumps(
            {
                "logs": [],
                "daily_invocation_counts": {"2024-08-01": 10},
                "daily_user_code_failure_counts": {"2024-08-01": 2},
            }
        )

        result = self.workflow_retriever._transform_workflow_summary(workflow_summarized)

        self.assertEqual(
            result,
            {
                "workflow_runtime_samples": [],
                "daily_invocation_counts": {"2024-08-01": 10},
                "daily_user_code_failure_counts": {"2024-08-01": 2},
                "start_hop_summary": {},
                "instance_summary": {},
            },
        )
        mock_construct_summaries.assert_called_once_with([])

    @patch.object(WorkflowRetriever, "_handle_single_execution_data_entry")
    def test_extend_start_hop_summary(self, mock_handle_single_execution):
        start_hop_summary = {
            "invoked": 0,
            "retrieved_wpd_at_function": 0,
            "workflow_placement_decision_size_gb": [],
            "at_redirector": {},
            "from_client": {
                "transfer_sizes_gb": [],
                "received_region": {},
            },
        }

        log = {
            "start_hop_info": {
                "destination": "region-1",
                "data_transfer_size_gb": 0.5,
                "latency_from_client_s": 2.0,
                "workflow_placement_decision": {"data_size_gb": 1.0, "retrieved_wpd_at_function": True},
                "redirector_execution_data": {"instance_name": "instance-1"},
            }
        }

        self.workflow_retriever._round_to_kb = MagicMock(return_value=0.5)
        self.workflow_retriever._extend_start_hop_summary(start_hop_summary, log)

        # Check that start_hop_summary was updated correctly
        self.assertEqual(start_hop_summary["invoked"], 1)
        self.assertEqual(start_hop_summary["retrieved_wpd_at_function"], 1)
        self.assertEqual(start_hop_summary["workflow_placement_decision_size_gb"], [1.0])
        self.assertIn("region-1", start_hop_summary["from_client"]["received_region"])
        self.assertIn(0.5, start_hop_summary["from_client"]["transfer_sizes_gb"])
        self.assertEqual(
            start_hop_summary["from_client"]["received_region"]["region-1"]["transfer_size_gb_to_transfer_latencies_s"][
                0.5
            ],
            [2.0],
        )
        mock_handle_single_execution.assert_called_once_with(
            {"instance_name": "instance-1"}, start_hop_summary["at_redirector"]
        )

    @patch.object(WorkflowRetriever, "_handle_execution_data")
    @patch.object(WorkflowRetriever, "_handle_region_to_region_transmission")
    def test_extend_instance_summary(self, mock_handle_region_transmission, mock_handle_execution):
        instance_summary = {}

        log = {"execution_data": [{}], "transmission_data": [{}]}

        self.workflow_retriever._extend_instance_summary(instance_summary, log)

        # Verify the handlers were called correctly
        mock_handle_execution.assert_called_once_with(log, instance_summary)
        mock_handle_region_transmission.assert_called_once_with(log, instance_summary)

    @patch.object(WorkflowRetriever, "_handle_single_execution_data_entry")
    def test_handle_execution_data(self, mock_handle_single_execution_entry):
        instance_summary = {}
        log = {
            "execution_data": [
                {"instance_name": "instance-1", "provider_region": "region-1"},
                {"instance_name": "instance-2", "provider_region": "region-2"},
            ]
        }

        self.workflow_retriever._handle_execution_data(log, instance_summary)

        # Check the number of calls
        self.assertEqual(mock_handle_single_execution_entry.call_count, 2)

        # Ensure the method was called with the correct arguments
        for execution_info in log["execution_data"]:
            mock_handle_single_execution_entry.assert_any_call(execution_info, instance_summary)

    def test_handle_single_execution_data_entry(self):
        instance_summary = {}
        execution_info = {
            "instance_name": "instance-1",
            "provider_region": "region-1",
            "cpu_utilization": 0.8,
            "duration_s": 50,
            "data_transfer_during_execution_gb": 0.2,
            "successor_data": {"successor-1": {"invocation_time_from_function_start_s": 5}},
        }

        self.workflow_retriever._handle_single_execution_data_entry(execution_info, instance_summary)

        # Verify that the instance_summary dictionary was updated correctly
        expected_instance_summary = {
            "instance-1": {
                "invocations": 1,
                "cpu_utilization": [0.8],
                "executions": {
                    "at_region": {
                        "region-1": [
                            {
                                "duration_s": 50,
                                "cpu_utilization": 0.8,
                                "data_transfer_during_execution_gb": 0.2,
                                "successor_invocations": {
                                    "successor-1": {"invocation_time_from_function_start_s": 5.0}
                                },
                            }
                        ]
                    },
                    "successor_instances": {"successor-1"},
                },
            }
        }

        self.assertEqual(instance_summary, expected_instance_summary)

    def test_handle_region_to_region_transmission(self):
        instance_summary = {}

        log = {
            "transmission_data": [
                {
                    "from_instance": "instance-1",
                    "to_instance": "instance-2",
                    "from_region": "region-1",
                    "to_region": "region-2",
                    "transmission_size_gb": 0.1,
                    "transmission_latency_s": 0.5,
                    "successor_invoked": True,
                    "from_direct_successor": True,
                }
            ]
        }

        self.workflow_retriever._round_to_kb = MagicMock(return_value=0.1)
        self.workflow_retriever._handle_region_to_region_transmission(log, instance_summary)

        # Verify that the instance_summary dictionary was updated correctly
        expected_instance_summary = {
            "instance-1": {
                "to_instance": {
                    "instance-2": {
                        "invoked": 1,
                        "non_executions": 0,
                        "invocation_probability": 0.0,
                        "sync_size_gb": [],
                        "sns_only_size_gb": [],
                        "transfer_sizes_gb": [0.1],
                        "regions_to_regions": {
                            "region-1": {
                                "region-2": {
                                    "transfer_size_gb_to_transfer_latencies_s": {"0.1": [0.5]},
                                    "best_fit_line": {},
                                }
                            }
                        },
                        "non_execution_info": {},
                    }
                }
            }
        }
        self.assertEqual(instance_summary, expected_instance_summary)

    def test_reorganize_start_hop_summary(self):
        start_hop_summary = {
            "invoked": 2,
            "retrieved_wpd_at_function": 1,
            "workflow_placement_decision_size_gb": [1.0, 1.5],
            "at_redirector": {},
            "from_client": {
                "received_region": {
                    "region-1": {
                        "transfer_size_gb_to_transfer_latencies_s": {0.5: [2.0]},
                    },
                    "region-2": {
                        "transfer_size_gb_to_transfer_latencies_s": {0.7: [1.5]},
                    },
                },
                "transfer_sizes_gb": [0.5, 0.7],
            },
        }

        with patch.object(
            self.workflow_retriever,
            "_calculate_best_fit_line",
            return_value={"slope_s": 0.0, "intercept_s": 1.5, "min_latency_s": 1.05, "max_latency_s": 1.95},
        ):
            self.workflow_retriever._reorganize_start_hop_summary(start_hop_summary)

        expected_start_hop_summary = {
            "invoked": 2,
            "retrieved_wpd_at_function": 1,
            "workflow_placement_decision_size_gb": 1.25,
            "at_redirector": {},
            "from_client": {
                "received_region": {
                    "region-1": {
                        "transfer_size_gb_to_transfer_latencies_s": {0.5: [2.0]},
                        "best_fit_line": {
                            "slope_s": 0.0,
                            "intercept_s": 1.5,
                            "min_latency_s": 1.05,
                            "max_latency_s": 1.95,
                        },
                    },
                    "region-2": {
                        "transfer_size_gb_to_transfer_latencies_s": {0.7: [1.5]},
                        "best_fit_line": {
                            "slope_s": 0.0,
                            "intercept_s": 1.5,
                            "min_latency_s": 1.05,
                            "max_latency_s": 1.95,
                        },
                    },
                },
                "transfer_sizes_gb": [0.5, 0.7],
            },
            "wpd_at_function_probability": 0.5,
        }
        self.assertEqual(start_hop_summary, expected_start_hop_summary)

    def test_reorganize_instance_summary(self):
        instance_summary = {
            "instance-1": {
                "cpu_utilization": [0.8, 0.9],
                "executions": {
                    "at_region": {
                        "region-1": [
                            {
                                "duration_s": 50,
                                "cpu_utilization": 0.8,
                                "data_transfer_during_execution_gb": 0.2,
                                "successor_invocations": {"successor-1": {"invocation_time_from_function_start_s": 5}},
                            }
                        ]
                    },
                    "successor_instances": {"successor-1"},
                },
                "to_instance": {
                    "instance-2": {
                        "invoked": 1,
                        "non_executions": 1,
                        "invocation_probability": 0.0,
                        "sync_size_gb": [0.1],
                        "sns_only_size_gb": [0.05],
                        "transfer_sizes_gb": [],
                        "regions_to_regions": {},
                        "non_execution_info": {},
                    }
                },
            }
        }

        with patch.object(self.workflow_retriever, "_summarize_execution_data"), patch.object(
            self.workflow_retriever, "_summarize_non_execution_data"
        ), patch.object(self.workflow_retriever, "_calculate_average_sync_table_and_sns_size"), patch.object(
            self.workflow_retriever, "_handle_missing_region_to_region_transmission_data"
        ):
            self.workflow_retriever._reorganize_instance_summary(instance_summary)

            # Verifying calls for internal methods
            self.workflow_retriever._summarize_execution_data.assert_called_once_with(instance_summary)
            self.workflow_retriever._summarize_non_execution_data.assert_called_once_with(instance_summary)
            self.workflow_retriever._calculate_average_sync_table_and_sns_size.assert_called_once_with(instance_summary)
            self.workflow_retriever._handle_missing_region_to_region_transmission_data.assert_called_once_with(
                instance_summary
            )

    def test_calculate_best_fit_line(self):
        transfer_size_to_transfer_latencies = {"0.1": [1.0, 1.1, 1.2], "0.2": [2.0, 2.1, 2.2], "0.3": [3.0, 3.1, 3.2]}

        best_fit_line = self.workflow_retriever._calculate_best_fit_line(transfer_size_to_transfer_latencies)

        expected_best_fit_line = {"slope_s": 10.0, "intercept_s": 0.1, "min_latency_s": 1.47, "max_latency_s": 2.73}

        self.assertAlmostEqual(best_fit_line["slope_s"], expected_best_fit_line["slope_s"], places=5)
        self.assertAlmostEqual(best_fit_line["intercept_s"], expected_best_fit_line["intercept_s"], places=5)
        self.assertAlmostEqual(best_fit_line["min_latency_s"], expected_best_fit_line["min_latency_s"], places=5)
        self.assertAlmostEqual(best_fit_line["max_latency_s"], expected_best_fit_line["max_latency_s"], places=5)

    def test_round_to_kb(self):
        number = 0.0009765625  # 1 KB in GB
        result = self.workflow_retriever._round_to_kb(number, round_to=1, round_up=True)
        self.assertEqual(result, 0.0009765625)  # Should round up to 1 KB

        result = self.workflow_retriever._round_to_kb(number, round_to=1, round_up=False)
        self.assertEqual(result, 0.0009765625)  # Should round to nearest 1 KB

    def test_round_to_ms(self):
        number = 0.001  # 1 ms in seconds
        result = self.workflow_retriever._round_to_ms(number, round_to=1, round_up=True)
        self.assertEqual(result, 0.001)  # Should round up to 1 ms

        result = self.workflow_retriever._round_to_ms(number, round_to=1, round_up=False)
        self.assertEqual(result, 0.001)  # Should round to nearest 1 ms


# Example of how to run the tests
if __name__ == "__main__":
    unittest.main()
