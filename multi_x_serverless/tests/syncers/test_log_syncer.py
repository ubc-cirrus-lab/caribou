import unittest
from unittest.mock import patch, MagicMock
from multi_x_serverless.syncers.log_syncer import LogSyncer
from multi_x_serverless.common.constants import DEPLOYMENT_MANAGER_RESOURCE_TABLE, GLOBAL_TIME_ZONE

import unittest
import json
from datetime import datetime
from collections import defaultdict
from typing import Any
from multi_x_serverless.common.constants import LOG_VERSION


class TestLogSyncer(unittest.TestCase):
    def setUp(self):
        self.syncer = LogSyncer()

    def test_initialize_workflow_summary_instance(self):
        result = self.syncer._initialize_workflow_summary_instance()
        self.assertEqual(result, {"instance_summary": {}, "total_invocations": 0})

    def test_get_last_synced_time(self):
        with patch.object(
            self.syncer.endpoints.get_datastore_client(), "get_last_value_from_sort_key_table"
        ) as mock_get_last_value:
            mock_get_last_value.return_value = ["2022-01-01 00:00:00,000000+00:00"]
            result = self.syncer._get_last_synced_time("workflow_id")
            self.assertEqual(result.year, 2022)

    def test_validate_deployment_manager_config(self):
        with self.assertRaises(Exception):
            self.syncer._validate_deployment_manager_config({}, "workflow_id")

    def test_initialize_instance_summary(self):
        workflow_summary_instance = {"instance_summary": {}}
        function_instance = "function1"
        provider_region = {"provider": "aws", "region": "us-east-1"}
        self.syncer._initialize_instance_summary(function_instance, provider_region, workflow_summary_instance)
        self.assertIn(function_instance, workflow_summary_instance["instance_summary"])

    def test_process_logs(self):
        logs = [
            f"TIME (2024-01-01 00:00:00,000000) LEVEL (info) MESSAGE (ENTRY_POINT: RUN_ID (123): Entry Point INSTANCE (function1) of workflow some called with PAYLOAD_SIZE (10) GB and INIT_LATENCY (100) ms) LOG_VERSION ({LOG_VERSION})",
            f"TIME (2024-01-01 00:02:00,000000) LEVEL (info) MESSAGE (INVOKED: RUN_ID (123) INSTANCE (function1)) LOG_VERSION ({LOG_VERSION})",
            f"TIME (2024-01-01 00:03:00,000000) LEVEL (info) MESSAGE (EXECUTED: RUN_ID (123) INSTANCE (function1) EXECUTION_TIME (100)) LOG_VERSION ({LOG_VERSION})",
            f"TIME (2024-01-01 00:04:00,000000) LEVEL (info) MESSAGE (INVOKED: RUN_ID (123) INSTANCE (function1)) LOG_VERSION ({LOG_VERSION})",
            f"TIME (2024-01-01 00:05:00,000000) LEVEL (info) MESSAGE (EXECUTED: RUN_ID (123) INSTANCE (function1) EXECUTION_TIME (200)) LOG_VERSION ({LOG_VERSION})",
        ]
        function_instance = "function1"
        provider_region = {"provider": "aws", "region": "us-east-1"}
        workflow_summary_instance = {
            "total_invocations": 0,
            "instance_summary": {
                function_instance: {
                    "invocation_count": 0,
                    "execution_summary": {
                        f'{provider_region["provider"]}:{provider_region["region"]}': {
                            "invocation_count": 0,
                            "runtime_samples": [],
                        }
                    },
                    "invocation_summary": {},
                }
            },
        }
        latency_summary: dict[str, dict[str, dict[str, dict[str, Any]]]] = defaultdict(
            lambda: defaultdict(lambda: defaultdict(dict))
        )
        latency_summary_successor_before_caller_store: dict[str, dict[str, dict[str, Any]]] = defaultdict(
            lambda: defaultdict(dict)
        )

        self.syncer._process_logs(
            logs,
            provider_region,
            workflow_summary_instance,
            latency_summary,
            latency_summary_successor_before_caller_store,
        )
        self.assertEqual(workflow_summary_instance["instance_summary"][function_instance]["invocation_count"], 2)
        self.assertEqual(
            workflow_summary_instance["instance_summary"][function_instance]["execution_summary"][
                f'{provider_region["provider"]}:{provider_region["region"]}'
            ]["invocation_count"],
            2,
        )
        self.assertEqual(
            workflow_summary_instance["instance_summary"]["function1"]["execution_summary"][
                f'{provider_region["provider"]}:{provider_region["region"]}'
            ]["init_data_transfer_size_samples"],
            [10],
        )
        self.assertEqual(
            workflow_summary_instance["instance_summary"]["function1"]["execution_summary"][
                f'{provider_region["provider"]}:{provider_region["region"]}'
            ]["init_latency_samples"],
            [100],
        )
        self.assertEqual(
            workflow_summary_instance["instance_summary"][function_instance]["execution_summary"][
                f'{provider_region["provider"]}:{provider_region["region"]}'
            ]["runtime_samples"],
            [100, 200],
        )

    def test_process_logs_with_invocation_summary(self):
        logs = [
            f"TIME (1) LEVEL (info) MESSAGE (ENTRY_POINT: RUN_ID (123): Entry Point INSTANCE (function1) of workflow some called with PAYLOAD_SIZE (10) GB and INIT_LATENCY (100) ms) LOG_VERSION ({LOG_VERSION})",
            f"TIME (2) LEVEL (info) MESSAGE (INVOKED: RUN_ID (123) INSTANCE (function1)) LOG_VERSION ({LOG_VERSION})",
            f"TIME (3) LEVEL (info) MESSAGE (EXECUTED: RUN_ID (123) INSTANCE (function1) EXECUTION_TIME (100)) LOG_VERSION ({LOG_VERSION})",
            f"TIME (4) LEVEL (info) MESSAGE (INVOKED: RUN_ID (123) INSTANCE (function1)) LOG_VERSION ({LOG_VERSION})",
            f"TIME (5) LEVEL (info) MESSAGE (EXECUTED: RUN_ID (123) INSTANCE (function1) EXECUTION_TIME (200)) LOG_VERSION ({LOG_VERSION})",
            f"TIME (6) LEVEL (info) MESSAGE (INVOKING_SUCCESSOR: RUN_ID (123): INSTANCE (function1) calling SUCCESSOR (function2) with PAYLOAD_SIZE (1) GB) LOG_VERSION ({LOG_VERSION})",
            f"TIME (7) LEVEL (info) MESSAGE (INVOKING_SUCCESSOR: RUN_ID (123): INSTANCE (function1) calling SUCCESSOR (function2) with PAYLOAD_SIZE (2) GB) LOG_VERSION ({LOG_VERSION})",
        ]
        function_instance = "function1"
        provider_region = {"provider": "aws", "region": "us-east-1"}
        workflow_summary_instance = {
            "instance_summary": {
                function_instance: {
                    "invocation_count": 0,
                    "execution_summary": {
                        f'{provider_region["provider"]}:{provider_region["region"]}': {
                            "invocation_count": 0,
                            "average_runtime": 0,
                            "tail_runtime": 0,
                        }
                    },
                    "invocation_summary": {},
                }
            }
        }
        entry_point_invocation_count = self.syncer._process_logs(logs, provider_region, workflow_summary_instance)
        self.assertEqual(entry_point_invocation_count, 1)
        self.assertEqual(workflow_summary_instance["instance_summary"][function_instance]["invocation_count"], 2)
        self.assertEqual(
            workflow_summary_instance["instance_summary"][function_instance]["execution_summary"][
                f'{provider_region["provider"]}:{provider_region["region"]}'
            ]["invocation_count"],
            2,
        )
        self.assertEqual(
            workflow_summary_instance["instance_summary"][function_instance]["execution_summary"][
                f'{provider_region["provider"]}:{provider_region["region"]}'
            ]["average_runtime"],
            0.15,
        )
        self.assertEqual(
            workflow_summary_instance["instance_summary"][function_instance]["execution_summary"][
                f'{provider_region["provider"]}:{provider_region["region"]}'
            ]["tail_runtime"],
            0.2,
        )
        self.assertEqual(
            workflow_summary_instance["instance_summary"][function_instance]["invocation_summary"]["function2"][
                "invocation_count"
            ],
            2,
        )
        self.assertEqual(
            workflow_summary_instance["instance_summary"][function_instance]["invocation_summary"]["function2"][
                "average_data_transfer_size"
            ],
            1.5,
        )

    def test_process_logs_with_invocation_summary(self):
        logs = [
            f"TIME (2024-01-01 00:00:00,000000) LEVEL (info) MESSAGE (ENTRY_POINT: RUN_ID (123): Entry Point INSTANCE (function1) of workflow some called with PAYLOAD_SIZE (10) GB and INIT_LATENCY (100) ms) LOG_VERSION ({LOG_VERSION})",
            f"TIME (2024-01-01 00:01:00,000000) LEVEL (info) MESSAGE (INVOKED: RUN_ID (123) INSTANCE (function1)) LOG_VERSION ({LOG_VERSION})",
            f"TIME (2024-01-01 00:02:00,000000) LEVEL (info) MESSAGE (EXECUTED: RUN_ID (123) INSTANCE (function1) EXECUTION_TIME (100)) LOG_VERSION ({LOG_VERSION})",
            f"TIME (2024-01-01 00:03:00,000000) LEVEL (info) MESSAGE (INVOKED: RUN_ID (123) INSTANCE (function1)) LOG_VERSION ({LOG_VERSION})",
            f"TIME (2024-01-01 00:04:00,000000) LEVEL (info) MESSAGE (EXECUTED: RUN_ID (123) INSTANCE (function1) EXECUTION_TIME (200)) LOG_VERSION ({LOG_VERSION})",
            f"TIME (2024-01-01 00:05:00,000000) LEVEL (info) MESSAGE (INVOKING_SUCCESSOR: RUN_ID (123): INSTANCE (function1) calling SUCCESSOR (function2) with PAYLOAD_SIZE (1) GB) LOG_VERSION ({LOG_VERSION})",
            f"TIME (2024-01-01 00:06:00,000000) LEVEL (info) MESSAGE (INVOKING_SUCCESSOR: RUN_ID (123): INSTANCE (function1) calling SUCCESSOR (function2) with PAYLOAD_SIZE (2) GB) LOG_VERSION ({LOG_VERSION})",
            f"TIME (2024-01-01 00:07:00,000000) LEVEL (info) MESSAGE (INVOKED: RUN_ID (123) INSTANCE (function2)) LOG_VERSION ({LOG_VERSION})",
            f"TIME (2024-01-01 00:08:00,000000) LEVEL (info) MESSAGE (EXECUTED: RUN_ID (123) INSTANCE (function2) EXECUTION_TIME (300)) LOG_VERSION ({LOG_VERSION})",
            f"TIME (2024-01-01 00:09:00,000000) LEVEL (info) MESSAGE (INVOKED: RUN_ID (123) INSTANCE (function2)) LOG_VERSION ({LOG_VERSION})",
            f"TIME (2024-01-01 00:10:00,000000) LEVEL (info) MESSAGE (EXECUTED: RUN_ID (123) INSTANCE (function2) EXECUTION_TIME (150)) LOG_VERSION ({LOG_VERSION})",
            f"TIME (2024-01-01 00:11:00,000000) LEVEL (info) MESSAGE (INVOKING_SUCCESSOR: RUN_ID (123): INSTANCE (function2) calling SUCCESSOR (function3) with PAYLOAD_SIZE (4) GB) LOG_VERSION ({LOG_VERSION})",
        ]
        provider_region = {"provider": "aws", "region": "us-east-1"}
        workflow_summary_instance = {"instance_summary": {}, "total_invocations": 1}
        latency_summary: dict[str, dict[str, dict[str, dict[str, Any]]]] = defaultdict(
            lambda: defaultdict(lambda: defaultdict(dict))
        )
        latency_summary_successor_before_caller_store: dict[str, dict[str, dict[str, Any]]] = defaultdict(
            lambda: defaultdict(dict)
        )

        self.syncer._process_logs(
            logs,
            provider_region,
            workflow_summary_instance,
            latency_summary,
            latency_summary_successor_before_caller_store,
        )
        self.assertEqual(workflow_summary_instance["instance_summary"]["function1"]["invocation_count"], 2)
        self.assertEqual(
            workflow_summary_instance["instance_summary"]["function1"]["execution_summary"][
                f'{provider_region["provider"]}:{provider_region["region"]}'
            ]["invocation_count"],
            2,
        )
        self.assertEqual(
            workflow_summary_instance["instance_summary"]["function1"]["execution_summary"][
                f'{provider_region["provider"]}:{provider_region["region"]}'
            ]["init_data_transfer_size_samples"],
            [10],
        )
        self.assertEqual(
            workflow_summary_instance["instance_summary"]["function1"]["execution_summary"][
                f'{provider_region["provider"]}:{provider_region["region"]}'
            ]["init_latency_samples"],
            [100],
        )
        self.assertEqual(
            workflow_summary_instance["instance_summary"]["function1"]["execution_summary"][
                f'{provider_region["provider"]}:{provider_region["region"]}'
            ]["runtime_samples"],
            [100, 200],
        )
        self.assertEqual(
            workflow_summary_instance["instance_summary"]["function1"]["invocation_summary"]["function2"][
                "invocation_count"
            ],
            2,
        )
        self.assertEqual(
            workflow_summary_instance["instance_summary"]["function1"]["invocation_summary"]["function2"][
                "data_transfer_samples"
            ],
            [1, 2],
        )
        self.assertEqual(workflow_summary_instance["instance_summary"]["function2"]["invocation_count"], 2)
        self.assertEqual(
            workflow_summary_instance["instance_summary"]["function2"]["execution_summary"][
                f'{provider_region["provider"]}:{provider_region["region"]}'
            ]["invocation_count"],
            2,
        )
        self.assertEqual(
            workflow_summary_instance["instance_summary"]["function2"]["execution_summary"][
                f'{provider_region["provider"]}:{provider_region["region"]}'
            ]["runtime_samples"],
            [300, 150],
        )
        self.assertEqual(
            workflow_summary_instance["instance_summary"]["function2"]["invocation_summary"]["function3"][
                "invocation_count"
            ],
            1,
        )
        self.assertEqual(
            workflow_summary_instance["instance_summary"]["function2"]["invocation_summary"]["function3"][
                "data_transfer_samples"
            ],
            [4],
        )

    @patch("multi_x_serverless.common.models.endpoints.Endpoints")
    @patch("multi_x_serverless.common.models.remote_client.remote_client_factory.RemoteClientFactory")
    def test_sync(self, mock_remote_client_factory, mock_endpoints):
        # Create mock objects for the methods that will be called
        mock_get_deployment_manager_client = MagicMock()
        mock_get_deployment_manager_client.get_all_values_from_table.return_value = {}
        mock_endpoints.get_deployment_manager_client.return_value = mock_get_deployment_manager_client

        mock_get_remote_client = MagicMock()
        mock_remote_client_factory.get_remote_client = mock_get_remote_client

        mock_remote_client = MagicMock()
        mock_get_remote_client.return_value = mock_remote_client

        # Create a DatastoreSyncer instance and call the sync method
        syncer = LogSyncer()
        syncer.endpoints = mock_endpoints
        syncer.sync()

        # Check if the methods were called with the correct arguments
        mock_endpoints.get_deployment_manager_client.assert_called_once()
        mock_get_deployment_manager_client.get_all_values_from_table.assert_called_once_with(
            DEPLOYMENT_MANAGER_RESOURCE_TABLE
        )

    @patch.object(LogSyncer, "process_workflow")
    @patch.object(LogSyncer, "_get_last_synced_time")
    @patch.object(LogSyncer, "_initialize_workflow_summary_instance")
    @patch("multi_x_serverless.common.models.endpoints.Endpoints")
    @patch("multi_x_serverless.common.models.remote_client.remote_client_factory.RemoteClientFactory")
    def test_sync(
        self,
        mock_remote_client_factory,
        mock_endpoints,
        mock_initialize_workflow_summary_instance,
        mock_get_last_synced_time,
        mock_process_workflow,
    ):
        # Mocking the scenario where the sync method is called successfully
        mock_get_deployment_manager_client = MagicMock()
        mock_get_deployment_manager_client.get_all_values_from_table.return_value = {
            "workflow_id": "deployment_manager_config_json"
        }
        mock_endpoints.get_deployment_manager_client.return_value = mock_get_deployment_manager_client

        mock_get_remote_client = MagicMock()
        mock_remote_client_factory.get_remote_client = mock_get_remote_client

        mock_remote_client = MagicMock()
        mock_get_remote_client.return_value = mock_remote_client

        mock_initialize_workflow_summary_instance.return_value = {"instance_summary": {}}
        mock_get_last_synced_time.return_value = datetime(2022, 1, 1)

        # Create a DatastoreSyncer instance and call the sync method
        syncer = LogSyncer()
        syncer.endpoints = mock_endpoints
        syncer.sync()

        # Check if the methods were called with the correct arguments
        mock_endpoints.get_deployment_manager_client.assert_called_once()
        mock_get_deployment_manager_client.get_all_values_from_table.assert_called_once_with(
            DEPLOYMENT_MANAGER_RESOURCE_TABLE
        )
        mock_process_workflow.assert_called_once_with("workflow_id", "deployment_manager_config_json")

    @patch.object(LogSyncer, "_process_function_instance")
    @patch.object(LogSyncer, "_validate_deployment_manager_config")
    def test_process_workflow(self, mock_validate_deployment_manager_config, mock_process_function_instance):
        # Mocking the scenario where the process_workflow method is called successfully
        syncer = LogSyncer()
        workflow_summary_instance = {"instance_summary": {}}
        syncer._initialize_workflow_summary_instance = MagicMock(return_value=workflow_summary_instance)
        syncer._get_last_synced_time = MagicMock(return_value=datetime(2022, 1, 1, tzinfo=GLOBAL_TIME_ZONE))
        syncer.endpoints.get_datastore_client().put_value_to_sort_key_table = MagicMock()

        deployment_manager_config_json = json.dumps(
            {
                "deployed_regions": json.dumps(
                    {"function_physical_instance": {"deploy_region": {"provider": "aws", "region": "us-east-1"}}}
                )
            }
        )

        mock_process_function_instance.return_value = 1

        # Call the method with test values
        syncer.process_workflow("workflow_id", deployment_manager_config_json)

        # Check that the validate_deployment_manager_config and process_function_instance methods were called
        mock_validate_deployment_manager_config.assert_called()
        mock_process_function_instance.assert_called()

    def test_extract_executed_logs(self):
        runtimes = {}
        self.syncer._extract_executed_logs("INSTANCE (function1) EXECUTION_TIME (1.23)", runtimes)
        self.assertEqual(runtimes, {"function1": [1.23]})

    def test_extract_from_string(self):
        result = self.syncer._extract_from_string("INSTANCE (function1) EXECUTION_TIME (1.23)", r"INSTANCE \((.*?)\)")
        self.assertEqual(result, "function1")

    def test_extract_invoking_successor_logs(self):
        workflow_summary_instance = {"instance_summary": {}}
        data_transfer_sizes = {}
        latency_summary = {}
        provider_region = {"provider": "aws", "region": "us-east-1"}
        self.syncer._extract_invoking_successor_logs(
            "INSTANCE (function1) SUCCESSOR (function2) PAYLOAD_SIZE (1.23)",
            provider_region,
            workflow_summary_instance,
            data_transfer_sizes,
            latency_summary,
            "run_id",
            1.23,
        )
        self.assertEqual(
            workflow_summary_instance["instance_summary"]["function1"]["invocation_summary"]["function2"][
                "invocation_count"
            ],
            1,
        )
        self.assertEqual(data_transfer_sizes, {"function1": {"function2": [1.23]}})

    def test_extract_invoked_logs(self):
        workflow_summary_instance = {"instance_summary": {}}
        latency_summary = defaultdict(lambda: defaultdict(lambda: defaultdict(dict)))
        latency_summary_successor_before_caller_store = defaultdict(lambda: defaultdict(dict))
        provider_region = {"provider": "aws", "region": "us-east-1"}
        self.syncer._extract_invoked_logs(
            "INSTANCE (function1)",
            provider_region,
            workflow_summary_instance,
            latency_summary,
            latency_summary_successor_before_caller_store,
            "run_id",
            1.23,
        )
        self.assertEqual(workflow_summary_instance["instance_summary"]["function1"]["invocation_count"], 1)

    def test_update_invoked_latency_summary(self):
        latency_summary = {"run_id": {"function1": {"function2": {}}}}
        latency_summary_successor_before_caller_store = {"run_id": {"function2": {}}}
        provider_region = {"provider": "aws", "region": "us-east-1"}
        self.syncer._update_invoked_latency_summary(
            "run_id",
            "function2",
            1.23,
            latency_summary,
            latency_summary_successor_before_caller_store,
            provider_region,
        )
        self.assertEqual(latency_summary["run_id"]["function1"]["function2"]["end_time"], 1.23)

    def test_update_invoking_successor_latency_summary(self):
        latency_summary = {}
        provider_region = {"provider": "aws", "region": "us-east-1"}
        self.syncer._update_invoking_successor_latency_summary(
            "run_id",
            "function1",
            "function2",
            1.23,
            latency_summary,
            provider_region,
        )
        self.assertEqual(latency_summary["run_id"]["function1"]["function2"]["start_time"], 1.23)

    def test_update_workflow_summary_with_latency_summary(self):
        workflow_summary_instance = {
            "instance_summary": {
                "caller": {"invocation_summary": {"callee": {"transmission_summary": {"outgoing": {"incoming": {}}}}}}
            }
        }
        latency_aggregates = {
            "caller": {
                "callee": {
                    "outgoing": {"incoming": {"average_latency": 1.23, "tail_latency": 4.56, "transmission_count": 7}}
                }
            }
        }
        self.syncer._update_workflow_summary_with_latency_summary(workflow_summary_instance, latency_aggregates)
        latency_summary = workflow_summary_instance["instance_summary"]["caller"]["invocation_summary"]["callee"][
            "transmission_summary"
        ]["outgoing"]["incoming"]
        self.assertEqual(latency_summary["average_latency"], 1.23)
        self.assertEqual(latency_summary["tail_latency"], 4.56)
        self.assertEqual(latency_summary["transmission_count"], 7)

    def test_cleanup_latency_summary(self):
        latency_summary = {
            "run_id": {
                "caller": {
                    "callee": {
                        "start_time": 1,
                        "end_time": None,
                        "outgoing_provider": "outgoing",
                        "incoming_provider": None,
                    }
                }
            }
        }
        latency_summary_successor_before_caller_store = {
            "run_id": {
                "callee": {
                    "end_time": 5,
                    "incoming_provider": "incoming",
                }
            }
        }
        result = self.syncer._cleanup_latency_summary(latency_summary, latency_summary_successor_before_caller_store)
        self.assertEqual(result["caller"]["callee"]["outgoing"]["incoming"]["latency_samples"], [4])
        self.assertEqual(result["caller"]["callee"]["outgoing"]["incoming"]["transmission_count"], 1)

    def test_cleanup_latency_summary_multiple_runs(self):
        latency_summary = {
            "run_id1": {
                "caller1": {
                    "callee1": {
                        "start_time": 5,
                        "end_time": None,
                        "outgoing_provider": "outgoing",
                        "incoming_provider": None,
                    }
                },
                "caller2": {
                    "callee2": {
                        "start_time": 6,
                        "end_time": None,
                        "outgoing_provider": "outgoing",
                        "incoming_provider": None,
                    }
                },
            },
            "run_id2": {
                "caller1": {
                    "callee1": {
                        "start_time": 9,
                        "end_time": None,
                        "outgoing_provider": "outgoing",
                        "incoming_provider": None,
                    }
                },
                "caller2": {
                    "callee2": {
                        "start_time": 10,
                        "end_time": None,
                        "outgoing_provider": "outgoing",
                        "incoming_provider": None,
                    }
                },
            },
        }
        latency_summary_successor_before_caller_store = {
            "run_id1": {
                "callee1": {
                    "end_time": 10,
                    "incoming_provider": "incoming",
                },
                "callee2": {
                    "end_time": 20,
                    "incoming_provider": "incoming",
                },
            },
            "run_id2": {
                "callee1": {
                    "end_time": 16,
                    "incoming_provider": "incoming",
                },
                "callee2": {
                    "end_time": 12,
                    "incoming_provider": "incoming",
                },
            },
        }
        result = self.syncer._cleanup_latency_summary(latency_summary, latency_summary_successor_before_caller_store)
        self.assertEqual(result["caller1"]["callee1"]["outgoing"]["incoming"]["latency_samples"], [5, 7])
        self.assertEqual(result["caller1"]["callee1"]["outgoing"]["incoming"]["transmission_count"], 2)
        self.assertEqual(result["caller2"]["callee2"]["outgoing"]["incoming"]["latency_samples"], [14, 2])
        self.assertEqual(result["caller2"]["callee2"]["outgoing"]["incoming"]["transmission_count"], 2)

    def test_cleanup_latency_summary_multiple_providers(self):
        latency_summary = {
            "run_id": {
                "caller": {
                    "callee1": {
                        "start_time": 1,
                        "end_time": None,
                        "outgoing_provider": "outgoing1",
                        "incoming_provider": None,
                    },
                    "callee2": {
                        "start_time": 6,
                        "end_time": None,
                        "outgoing_provider": "outgoing2",
                        "incoming_provider": None,
                    },
                }
            }
        }
        latency_summary_successor_before_caller_store = {
            "run_id": {
                "callee1": {
                    "end_time": 4,
                    "incoming_provider": "incoming1",
                },
                "callee2": {
                    "end_time": 10,
                    "incoming_provider": "incoming2",
                },
            }
        }
        result = self.syncer._cleanup_latency_summary(latency_summary, latency_summary_successor_before_caller_store)
        self.assertEqual(result["caller"]["callee1"]["outgoing1"]["incoming1"]["latency_samples"], [3])
        self.assertEqual(result["caller"]["callee1"]["outgoing1"]["incoming1"]["transmission_count"], 1)
        self.assertEqual(result["caller"]["callee2"]["outgoing2"]["incoming2"]["latency_samples"], [4])
        self.assertEqual(result["caller"]["callee2"]["outgoing2"]["incoming2"]["transmission_count"], 1)


if __name__ == "__main__":
    unittest.main()
