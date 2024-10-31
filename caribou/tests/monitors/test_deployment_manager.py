import unittest
import json
from unittest.mock import patch, MagicMock, call
import numpy as np
from datetime import datetime, timedelta
from caribou.monitors.deployment_manager import DeploymentManager
from caribou.common.constants import (
    DEPLOYMENT_MANAGER_WORKFLOW_INFO_TABLE,
    TIME_FORMAT,
    DEFAULT_MONITOR_COOLDOWN,
    TIME_FORMAT_DAYS,
    FORGETTING_TIME_DAYS,
    GLOBAL_TIME_ZONE,
    CARBON_REGION_TABLE,
    CARBON_INTENSITY_TO_INVOCATION_SECOND_ESTIMATE,
    COARSE_GRAINED_DEPLOYMENT_ALGORITHM_CARBON_PER_INSTANCE_INVOCATION_ESTIMATE,
    GLOBAL_SYSTEM_REGION,
    DEPLOYMENT_MANAGER_RESOURCE_TABLE,
    WORKFLOW_INSTANCE_TABLE,
)


class TestDeploymentManager(unittest.TestCase):
    @patch("caribou.monitors.deployment_manager.WorkflowCollector")
    def setUp(self, mock_workflow_collector):
        self.deployment_manager = DeploymentManager()
        self.mock_workflow_collector = mock_workflow_collector
        self.mock_endpoints = MagicMock()
        self.deployment_manager._endpoints = self.mock_endpoints

    @patch("caribou.monitors.deployment_manager.datetime")
    def test_update_workflow_info(self, mock_datetime):
        self.deployment_manager._get_sigmoid_scale = MagicMock(return_value=1)
        # Arrange
        mock_datetime.now.return_value = datetime(2022, 1, 1)
        mock_client = MagicMock()
        self.mock_endpoints.get_deployment_manager_client.return_value = mock_client

        # Act
        self.deployment_manager._update_workflow_info(10, "workflow1")

        # Assert
        self.deployment_manager._get_sigmoid_scale.assert_called_once_with(10)
        self.mock_endpoints.get_deployment_manager_client.assert_called_once()
        mock_client.set_value_in_table.assert_called_once_with(
            DEPLOYMENT_MANAGER_WORKFLOW_INFO_TABLE,
            "workflow1",
            json.dumps(
                {
                    "next_check": (datetime(2022, 1, 1) + timedelta(seconds=int(DEFAULT_MONITOR_COOLDOWN))).strftime(
                        TIME_FORMAT
                    )
                }
            ),
        )

    @patch("caribou.monitors.deployment_manager.datetime")
    def test_get_sigmoid_scale(self, mock_datetime):
        # Arrange
        x = 10

        # Act
        result = self.deployment_manager._get_sigmoid_scale(x)

        # Assert
        self.assertAlmostEqual(result, 0.6495019919374339)

    def test_get_last_solved(self):
        # Arrange
        workflow_info = {"last_solved": "2022-01-01 00:00:00,000+00:00"}

        # Act
        result = self.deployment_manager._get_last_solved(workflow_info)

        # Assert
        self.assertEqual(
            result, datetime(2022, 1, 1, tzinfo=GLOBAL_TIME_ZONE)
        )  # This is the expected result for the given workflow_info

    @patch("caribou.monitors.deployment_manager.datetime")
    def test_get_last_solved_none(self, mock_datetime):
        # Arrange
        mock_datetime.now.return_value = datetime(2022, 1, 1)
        workflow_info = None

        # Act
        result = self.deployment_manager._get_last_solved(workflow_info)

        # Assert
        self.assertEqual(
            result, datetime(2022, 1, 1) - timedelta(days=FORGETTING_TIME_DAYS)
        )  # This is the expected result when workflow_info is None

    def test_get_total_invocation_counts_since_last_solved(self):
        # Arrange
        workflow_summary = {
            "daily_invocation_counts": {
                datetime(2022, 1, 1, tzinfo=GLOBAL_TIME_ZONE).strftime(TIME_FORMAT_DAYS): 10,
                datetime(2022, 1, 2, tzinfo=GLOBAL_TIME_ZONE).strftime(TIME_FORMAT_DAYS): 20,
                datetime(2022, 1, 3, tzinfo=GLOBAL_TIME_ZONE).strftime(TIME_FORMAT_DAYS): 30,
            }
        }
        last_solved = datetime(2022, 1, 2, tzinfo=GLOBAL_TIME_ZONE)

        # Act
        result = self.deployment_manager._get_total_invocation_counts_since_last_solved(workflow_summary, last_solved)

        # Assert
        self.assertEqual(result, 30)  # This is the expected result for the given workflow_summary and last_solved

    def test_calculate_positive_carbon_savings_token(self):
        # Arrange
        home_region = "region1"
        workflow_summary = {"runtime_avg": 2}
        total_invocation_counts_since_last_solved = 10
        self.deployment_manager._get_potential_carbon_savings_per_invocation_s = MagicMock(return_value=1)
        self.deployment_manager._get_runtime_avg = MagicMock(return_value=2)

        # Act
        result = self.deployment_manager._calculate_positive_carbon_savings_token(
            home_region, workflow_summary, total_invocation_counts_since_last_solved
        )

        # Assert
        self.assertEqual(result, 20)
        self.deployment_manager._get_potential_carbon_savings_per_invocation_s.assert_called_once_with(home_region)
        self.deployment_manager._get_runtime_avg.assert_called_once_with(workflow_summary)

    def test_get_potential_carbon_savings_per_invocation_s(self):
        # Arrange
        home_region = "region1"
        mock_client = MagicMock()
        self.mock_endpoints.get_deployment_manager_client.return_value = mock_client
        mock_client.get_value_from_table.side_effect = [
            (
                json.dumps(
                    {
                        "transmission_distances": {
                            "region1": 1000,
                            "region2": 5000,
                            "region3": 3000,
                        }
                    }
                ),
                0.0,
            ),
            (json.dumps({"averages": {"overall": {"carbon_intensity": 1}}}), 0.0),
            (json.dumps({"averages": {"overall": {"carbon_intensity": 2}}}), 0.0),
        ]

        # Act
        result = self.deployment_manager._get_potential_carbon_savings_per_invocation_s(home_region)

        # Assert
        self.assertEqual(result, np.std([1, 2]) * CARBON_INTENSITY_TO_INVOCATION_SECOND_ESTIMATE)
        self.mock_endpoints.get_deployment_manager_client.assert_called()
        mock_client.get_value_from_table.assert_has_calls(
            [
                call(CARBON_REGION_TABLE, home_region),
                call(CARBON_REGION_TABLE, "region1"),
                call(CARBON_REGION_TABLE, "region3"),
            ]
        )

    def test_get_runtime_avg(self):
        # Arrange
        workflow_summary = {"workflow_runtime_samples": [1, 2, 3, 4, 5]}

        # Act
        result = self.deployment_manager._get_runtime_avg(workflow_summary)

        # Assert
        self.assertEqual(result, 3)  # This is the expected result for the given workflow_summary

    def test_calculate_affordable_deployment_algorithm_run(self):
        # Arrange
        number_of_instances = 10
        token_budget = 100
        self.deployment_manager._get_cost = MagicMock(side_effect=[50, 150, 30, 120])

        # Act
        result = self.deployment_manager._calculate_affordable_deployment_algorithm_run(
            number_of_instances, token_budget
        )

        # Assert
        self.assertEqual(
            result,
            {
                "number_of_solves": 1,
                "algorithm": "coarse_grained_deployment_algorithm",
                "leftover_tokens": 50,
            },
        )  # This is the expected result for the given number_of_instances and token_budget
        self.deployment_manager._get_cost.assert_has_calls(
            [
                call(
                    number_of_instances, 1, COARSE_GRAINED_DEPLOYMENT_ALGORITHM_CARBON_PER_INSTANCE_INVOCATION_ESTIMATE
                ),
                call(
                    number_of_instances, 2, COARSE_GRAINED_DEPLOYMENT_ALGORITHM_CARBON_PER_INSTANCE_INVOCATION_ESTIMATE
                ),
            ]
        )

    def test_get_cost(self):
        # Arrange
        number_of_instances = 10
        number_of_solves = 2
        algorithm_estimate = 1
        self.deployment_manager._get_carbon_intensity_system = MagicMock(return_value=1)

        # Act
        result = self.deployment_manager._get_cost(number_of_instances, number_of_solves, algorithm_estimate)

        # Assert
        self.assertEqual(
            result, 20
        )  # This is the expected result for the given number_of_instances, number_of_solves, and algorithm_estimate
        self.deployment_manager._get_carbon_intensity_system.assert_called_once()

    def test_get_carbon_intensity_system(self):
        # Arrange
        mock_client = MagicMock()
        self.mock_endpoints.get_deployment_manager_client.return_value = mock_client
        mock_client.get_value_from_table.return_value = (
            json.dumps({"averages": {"overall": {"carbon_intensity": 1}}}),
            0.0,
        )

        # Act
        result = self.deployment_manager._get_carbon_intensity_system()

        # Assert
        self.assertEqual(result, 1)  # This is the expected result
        self.mock_endpoints.get_deployment_manager_client.assert_called_once()
        mock_client.get_value_from_table.assert_called_once_with(CARBON_REGION_TABLE, f"aws:{GLOBAL_SYSTEM_REGION}")

    def test_get_solve_hours(self):
        # Arrange
        number_of_solves = 4

        # Act
        result = self.deployment_manager._get_solve_hours(number_of_solves)

        # Assert
        self.assertEqual(result, ["0", "6", "12", "18"])  # This is the expected result for the given number_of_solves

    def test_calculate_expiry_delta_seconds(self):
        # Arrange
        tokens_left = 10
        self.deployment_manager._get_sigmoid_scale = MagicMock(return_value=1)

        # Act
        result = self.deployment_manager._calculate_expiry_delta_seconds(tokens_left)

        # Assert
        self.assertEqual(result, int(DEFAULT_MONITOR_COOLDOWN))
        self.deployment_manager._get_sigmoid_scale.assert_called_once_with(tokens_left)

    def test_calculate_expiry_delta_seconds_with_scale(self):
        # Arrange
        tokens_left = 10
        self.deployment_manager._get_sigmoid_scale = MagicMock(return_value=0.5)

        # Act
        result = self.deployment_manager._calculate_expiry_delta_seconds(tokens_left)

        # Assert
        self.assertEqual(result, int(DEFAULT_MONITOR_COOLDOWN * 0.5))
        self.deployment_manager._get_sigmoid_scale.assert_called_once_with(tokens_left)

    @patch("caribou.monitors.deployment_manager.datetime")
    def test_upload_new_workflow_info(self, mock_datetime):
        # Arrange
        self.deployment_manager._calculate_expiry_delta_seconds = MagicMock(return_value=3600)
        mock_datetime.now.return_value = datetime(2022, 1, 1, tzinfo=GLOBAL_TIME_ZONE)
        mock_client = MagicMock()
        self.mock_endpoints.get_deployment_manager_client.return_value = mock_client

        # Act
        self.deployment_manager._upload_new_workflow_info(10, "workflow1")

        # Assert
        self.deployment_manager._calculate_expiry_delta_seconds.assert_called_once_with(10)
        self.mock_endpoints.get_deployment_manager_client.assert_called_once()
        mock_client.set_value_in_table.assert_called_once_with(
            DEPLOYMENT_MANAGER_WORKFLOW_INFO_TABLE,
            "workflow1",
            json.dumps(
                {
                    "last_solved": datetime(2022, 1, 1, tzinfo=GLOBAL_TIME_ZONE).strftime(TIME_FORMAT),
                    "tokens_left": 10,
                    "next_check": (datetime(2022, 1, 1, tzinfo=GLOBAL_TIME_ZONE) + timedelta(seconds=3600)).strftime(
                        TIME_FORMAT
                    ),
                }
            ),
        )

    @patch("caribou.monitors.deployment_manager.DeploymentManager.remote_check_workflow")
    @patch("caribou.monitors.deployment_manager.DeploymentManager.check_workflow")
    def test_check(self, mock_check_workflow, mock_remote_check_workflow):
        # Arrange
        mock_client = MagicMock()
        self.mock_endpoints.get_deployment_manager_client.return_value = mock_client
        mock_client.get_keys.return_value = ["workflow1", "workflow2"]

        # Act
        self.deployment_manager.check()

        # Assert
        self.mock_endpoints.get_deployment_manager_client.assert_called_once()
        mock_client.get_keys.assert_called_once_with(DEPLOYMENT_MANAGER_RESOURCE_TABLE)
        mock_check_workflow.assert_has_calls([call("workflow1"), call("workflow2")])
        mock_remote_check_workflow.assert_not_called()

    @patch("caribou.monitors.deployment_manager.DeploymentManager.remote_check_workflow")
    @patch("caribou.monitors.deployment_manager.DeploymentManager.check_workflow")
    def test_check_deployed_remotely(self, mock_check_workflow, mock_remote_check_workflow):
        # Arrange
        self.deployment_manager._deployed_remotely = True
        mock_client = MagicMock()
        self.mock_endpoints.get_deployment_manager_client.return_value = mock_client
        mock_client.get_keys.return_value = ["workflow1", "workflow2"]

        # Act
        self.deployment_manager.check()

        # Assert
        self.mock_endpoints.get_deployment_manager_client.assert_called_once()
        mock_client.get_keys.assert_called_once_with(DEPLOYMENT_MANAGER_RESOURCE_TABLE)
        mock_remote_check_workflow.assert_has_calls([call("workflow1"), call("workflow2")])
        mock_check_workflow.assert_not_called()

    def test_remote_check_workflow(self):
        # Arrange
        workflow_id = "workflow1"
        mock_framework_cli_remote_client = MagicMock()

        mock_endpoints = MagicMock()
        mock_endpoints.get_framework_cli_remote_client.return_value = mock_framework_cli_remote_client

        self.deployment_manager._endpoints = mock_endpoints

        # Act
        self.deployment_manager.remote_check_workflow(workflow_id)

        # Assert
        mock_endpoints.get_framework_cli_remote_client.assert_called_once()
        mock_framework_cli_remote_client.invoke_remote_framework_internal_action.assert_called_once_with(
            "check_workflow",
            {
                "workflow_id": workflow_id,
                "deployment_metrics_calculator_type": self.deployment_manager._deployment_metrics_calculator_type,
            },
        )

    def test_remote_run_deployment_algorithm(self):
        # Arrange
        workflow_id = "workflow1"
        solve_hours = ["0", "6", "12", "18"]
        leftover_tokens = 10
        mock_framework_cli_remote_client = MagicMock()

        mock_endpoints = MagicMock()
        mock_endpoints.get_framework_cli_remote_client.return_value = mock_framework_cli_remote_client

        self.deployment_manager._endpoints = mock_endpoints

        # Act
        self.deployment_manager.remote_run_deployment_algorithm(workflow_id, solve_hours, leftover_tokens)

        # Assert
        mock_endpoints.get_framework_cli_remote_client.assert_called_once()
        mock_framework_cli_remote_client.invoke_remote_framework_internal_action.assert_called_once_with(
            "run_deployment_algorithm",
            {
                "workflow_id": workflow_id,
                "solve_hours": solve_hours,
                "leftover_tokens": leftover_tokens,
                "deployment_metrics_calculator_type": self.deployment_manager._deployment_metrics_calculator_type,
            },
        )

    @patch("caribou.monitors.deployment_manager.datetime")
    @patch("caribou.monitors.deployment_manager.DeploymentManager._get_workflow_config")
    @patch("caribou.monitors.deployment_manager.DeploymentManager._get_last_solved")
    @patch("caribou.monitors.deployment_manager.DeploymentManager._get_total_invocation_counts_since_last_solved")
    @patch("caribou.monitors.deployment_manager.DeploymentManager._calculate_positive_carbon_savings_token")
    @patch("caribou.monitors.deployment_manager.DeploymentManager._calculate_affordable_deployment_algorithm_run")
    @patch("caribou.monitors.deployment_manager.DeploymentManager._get_cost")
    @patch("caribou.monitors.deployment_manager.DeploymentManager._update_workflow_info")
    @patch("caribou.monitors.deployment_manager.DeploymentManager.run_deployment_algorithm")
    @patch("caribou.monitors.deployment_manager.DeploymentManager.remote_run_deployment_algorithm")
    def test_check_workflow(
        self,
        mock_remote_run_deployment_algorithm,
        mock_run_deployment_algorithm,
        mock_update_workflow_info,
        mock_get_cost,
        mock_calculate_affordable_deployment_algorithm_run,
        mock_calculate_positive_carbon_savings_token,
        mock_get_total_invocation_counts_since_last_solved,
        mock_get_last_solved,
        mock_get_workflow_config,
        mock_datetime,
    ):
        # Arrange
        workflow_id = "workflow1"
        mock_datetime.now.return_value = datetime(2022, 1, 2, tzinfo=GLOBAL_TIME_ZONE)
        mock_datetime.strptime = datetime.strptime
        mock_client = MagicMock()
        self.mock_endpoints.get_deployment_manager_client.return_value = mock_client
        self.mock_endpoints.get_data_collector_client.return_value = mock_client
        mock_client.get_value_from_table.side_effect = [
            (json.dumps({"next_check": "2022-01-01 00:00:00,000+00:00"}), 0.0),  # workflow_info_raw
            (
                json.dumps({"workflow_config": json.dumps({"home_region": "region1", "instances": [1, 2, 3]})}),
                0.0,
            ),  # workflow_config_from_table
            (json.dumps({}), 0.0),  # workflow_summary_raw
        ]
        mock_get_last_solved.return_value = datetime(2022, 1, 1)
        mock_get_total_invocation_counts_since_last_solved.return_value = 10
        mock_calculate_positive_carbon_savings_token.return_value = 100
        mock_calculate_affordable_deployment_algorithm_run.return_value = {
            "number_of_solves": 1,
            "leftover_tokens": 50,
        }
        mock_get_workflow_config.return_value = MagicMock()

        # Act
        self.deployment_manager.check_workflow(workflow_id)

        # Assert
        self.mock_endpoints.get_deployment_manager_client.assert_called()
        mock_client.get_value_from_table.assert_has_calls(
            [
                call(DEPLOYMENT_MANAGER_WORKFLOW_INFO_TABLE, workflow_id),
                call(WORKFLOW_INSTANCE_TABLE, workflow_id, convert_from_bytes=True),
            ]
        )
        mock_get_last_solved.assert_called_once()
        mock_get_total_invocation_counts_since_last_solved.assert_called_once()
        mock_calculate_positive_carbon_savings_token.assert_called_once()
        mock_calculate_affordable_deployment_algorithm_run.assert_called_once()
        mock_get_cost.assert_not_called()
        mock_update_workflow_info.assert_not_called()
        mock_run_deployment_algorithm.assert_called_once()
        mock_remote_run_deployment_algorithm.assert_not_called()

    @patch("caribou.monitors.deployment_manager.datetime")
    @patch("caribou.monitors.deployment_manager.DeploymentManager._get_workflow_config")
    @patch("caribou.monitors.deployment_manager.DeploymentManager._get_last_solved")
    @patch("caribou.monitors.deployment_manager.DeploymentManager._get_total_invocation_counts_since_last_solved")
    @patch("caribou.monitors.deployment_manager.DeploymentManager._calculate_positive_carbon_savings_token")
    @patch("caribou.monitors.deployment_manager.DeploymentManager._calculate_affordable_deployment_algorithm_run")
    @patch("caribou.monitors.deployment_manager.DeploymentManager._get_cost")
    @patch("caribou.monitors.deployment_manager.DeploymentManager._update_workflow_info")
    @patch("caribou.monitors.deployment_manager.DeploymentManager.run_deployment_algorithm")
    @patch("caribou.monitors.deployment_manager.DeploymentManager.remote_run_deployment_algorithm")
    def test_check_workflow_not_enough_tokens(
        self,
        mock_remote_run_deployment_algorithm,
        mock_run_deployment_algorithm,
        mock_update_workflow_info,
        mock_get_cost,
        mock_calculate_affordable_deployment_algorithm_run,
        mock_calculate_positive_carbon_savings_token,
        mock_get_total_invocation_counts_since_last_solved,
        mock_get_last_solved,
        mock_get_workflow_config,
        mock_datetime,
    ):
        # Arrange
        workflow_id = "workflow1"
        mock_datetime.now.return_value = datetime(2022, 1, 2, tzinfo=GLOBAL_TIME_ZONE)
        mock_datetime.strptime = datetime.strptime
        mock_client = MagicMock()
        self.mock_endpoints.get_deployment_manager_client.return_value = mock_client
        self.mock_endpoints.get_data_collector_client.return_value = mock_client
        mock_client.get_value_from_table.side_effect = [
            (json.dumps({"next_check": "2022-01-01 00:00:00,000+00:00"}), 0.0),  # workflow_info_raw
            (
                json.dumps({"workflow_config": json.dumps({"home_region": "region1", "instances": [1, 2, 3]})}),
                0.0,
            ),  # workflow_config_from_table
            (json.dumps({}), 0.0),  # workflow_summary_raw
        ]
        mock_get_last_solved.return_value = datetime(2022, 1, 1)
        mock_get_total_invocation_counts_since_last_solved.return_value = 10
        mock_calculate_positive_carbon_savings_token.return_value = 100
        mock_calculate_affordable_deployment_algorithm_run.return_value = None
        mock_get_cost.return_value = 50
        mock_get_workflow_config.return_value = MagicMock()

        # Act
        self.deployment_manager.check_workflow(workflow_id)

        # Assert
        self.mock_endpoints.get_deployment_manager_client.assert_called()
        mock_client.get_value_from_table.assert_has_calls(
            [
                call(DEPLOYMENT_MANAGER_WORKFLOW_INFO_TABLE, workflow_id),
                call(WORKFLOW_INSTANCE_TABLE, workflow_id, convert_from_bytes=True),
            ]
        )
        mock_get_last_solved.assert_called_once()
        mock_get_total_invocation_counts_since_last_solved.assert_called_once()
        mock_calculate_positive_carbon_savings_token.assert_called_once()
        mock_calculate_affordable_deployment_algorithm_run.assert_called_once()
        mock_get_cost.assert_called_once()
        mock_update_workflow_info.assert_called_once()
        mock_run_deployment_algorithm.assert_not_called()
        mock_remote_run_deployment_algorithm.assert_not_called()

    @patch("caribou.monitors.deployment_manager.datetime")
    @patch("caribou.monitors.deployment_manager.DeploymentManager._get_workflow_config")
    @patch("caribou.monitors.deployment_manager.DeploymentManager._get_last_solved")
    @patch("caribou.monitors.deployment_manager.DeploymentManager._get_total_invocation_counts_since_last_solved")
    @patch("caribou.monitors.deployment_manager.DeploymentManager._calculate_positive_carbon_savings_token")
    @patch("caribou.monitors.deployment_manager.DeploymentManager._calculate_affordable_deployment_algorithm_run")
    @patch("caribou.monitors.deployment_manager.DeploymentManager._get_cost")
    @patch("caribou.monitors.deployment_manager.DeploymentManager._update_workflow_info")
    @patch("caribou.monitors.deployment_manager.DeploymentManager.run_deployment_algorithm")
    @patch("caribou.monitors.deployment_manager.DeploymentManager.remote_run_deployment_algorithm")
    def test_check_workflow_deployed_remotely(
        self,
        mock_remote_run_deployment_algorithm,
        mock_run_deployment_algorithm,
        mock_update_workflow_info,
        mock_get_cost,
        mock_calculate_affordable_deployment_algorithm_run,
        mock_calculate_positive_carbon_savings_token,
        mock_get_total_invocation_counts_since_last_solved,
        mock_get_last_solved,
        mock_get_workflow_config,
        mock_datetime,
    ):
        # Arrange
        self.deployment_manager._deployed_remotely = True
        workflow_id = "workflow1"
        mock_datetime.now.return_value = datetime(2022, 1, 2, tzinfo=GLOBAL_TIME_ZONE)
        mock_datetime.strptime = datetime.strptime
        mock_client = MagicMock()
        self.mock_endpoints.get_deployment_manager_client.return_value = mock_client
        self.mock_endpoints.get_data_collector_client.return_value = mock_client
        mock_client.get_value_from_table.side_effect = [
            (json.dumps({"next_check": "2022-01-01 00:00:00,000+00:00"}), 0.0),  # workflow_info_raw
            (
                json.dumps({"workflow_config": json.dumps({"home_region": "region1", "instances": [1, 2, 3]})}),
                0.0,
            ),  # workflow_config_from_table
            (json.dumps({}), 0.0),  # workflow_summary_raw
        ]
        mock_get_last_solved.return_value = datetime(2022, 1, 1)
        mock_get_total_invocation_counts_since_last_solved.return_value = 10
        mock_calculate_positive_carbon_savings_token.return_value = 100
        mock_calculate_affordable_deployment_algorithm_run.return_value = {
            "number_of_solves": 1,
            "leftover_tokens": 50,
        }
        mock_get_workflow_config.return_value = MagicMock()

        # Act
        self.deployment_manager.check_workflow(workflow_id)

        # Assert
        self.mock_endpoints.get_deployment_manager_client.assert_called()
        mock_client.get_value_from_table.assert_has_calls(
            [
                call(DEPLOYMENT_MANAGER_WORKFLOW_INFO_TABLE, workflow_id),
                call(WORKFLOW_INSTANCE_TABLE, workflow_id, convert_from_bytes=True),
            ]
        )
        mock_get_last_solved.assert_called_once()
        mock_get_total_invocation_counts_since_last_solved.assert_called_once()
        mock_calculate_positive_carbon_savings_token.assert_called_once()
        mock_calculate_affordable_deployment_algorithm_run.assert_called_once()
        mock_get_cost.assert_not_called()
        mock_update_workflow_info.assert_not_called()
        mock_run_deployment_algorithm.assert_not_called()
        mock_remote_run_deployment_algorithm.assert_called_once()

    @patch("caribou.monitors.deployment_manager.DeploymentManager._calculate_expiry_delta_seconds")
    @patch("caribou.monitors.deployment_manager.DeploymentManager._get_workflow_config")
    @patch("caribou.monitors.deployment_manager.DeploymentManager._run_deployment_algorithm")
    @patch("caribou.monitors.deployment_manager.DeploymentManager._upload_new_workflow_info")
    def test_run_deployment_algorithm(
        self,
        mock_upload_new_workflow_info,
        mock_run_deployment_algorithm,
        mock_get_workflow_config,
        mock_calculate_expiry_delta_seconds,
    ):
        # Arrange
        workflow_id = "workflow1"
        solve_hours = ["0", "6", "12", "18"]
        leftover_tokens = 10
        mock_calculate_expiry_delta_seconds.return_value = 3600
        mock_get_workflow_config.return_value = MagicMock()

        # Act
        self.deployment_manager.run_deployment_algorithm(workflow_id, solve_hours, leftover_tokens)

        # Assert
        mock_calculate_expiry_delta_seconds.assert_called_once_with(leftover_tokens)
        mock_get_workflow_config.assert_called_once_with(workflow_id)
        mock_run_deployment_algorithm.assert_called_once_with(mock_get_workflow_config.return_value, solve_hours, 3600)
        mock_upload_new_workflow_info.assert_called_once_with(leftover_tokens, workflow_id)

    @patch("caribou.monitors.deployment_manager.WorkflowConfig")
    def test_get_workflow_config(self, mock_workflow_config):
        # Arrange
        workflow_id = "workflow1"
        mock_client = MagicMock()
        self.mock_endpoints.get_data_collector_client.return_value = mock_client
        workflow_config_from_table = json.dumps({"workflow_config": json.dumps({"key": "value"})})
        mock_client.get_value_from_table.return_value = (workflow_config_from_table, 0.0)

        # Act
        result = self.deployment_manager._get_workflow_config(workflow_id)

        # Assert
        self.mock_endpoints.get_data_collector_client.assert_called_once()
        mock_client.get_value_from_table.assert_called_once_with(DEPLOYMENT_MANAGER_RESOURCE_TABLE, workflow_id)
        mock_workflow_config.assert_called_once_with({"key": "value"})
        self.assertEqual(result, mock_workflow_config.return_value)

    def test_get_workflow_config_invalid_config(self):
        # Arrange
        workflow_id = "workflow1"
        mock_client = MagicMock()
        self.mock_endpoints.get_data_collector_client.return_value = mock_client
        workflow_config_from_table = json.dumps({})
        mock_client.get_value_from_table.return_value = (workflow_config_from_table, 0.0)

        # Act & Assert
        with self.assertRaises(ValueError) as context:
            self.deployment_manager._get_workflow_config(workflow_id)

        self.assertEqual(str(context.exception), "Invalid workflow config")
        self.mock_endpoints.get_data_collector_client.assert_called_once()
        mock_client.get_value_from_table.assert_called_once_with(DEPLOYMENT_MANAGER_RESOURCE_TABLE, workflow_id)


if __name__ == "__main__":
    unittest.main()
