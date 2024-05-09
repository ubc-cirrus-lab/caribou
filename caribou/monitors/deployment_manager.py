import json
import math
import pdb
from datetime import datetime, timedelta
from typing import Optional

import numpy as np

from caribou.common.constants import (
    CARBON_INTENSITY_TO_INVOCATION_SECOND_ESTIMATE,
    CARBON_REGION_TABLE,
    COARSE_GRAINED_DEPLOYMENT_ALGORITHM_CARBON_PER_INSTANCE_INVOCATION_ESTIMATE,
    DEFAULT_MONITOR_COOLDOWN,
    DEPLOYMENT_MANAGER_RESOURCE_TABLE,
    DEPLOYMENT_MANAGER_WORKFLOW_INFO_TABLE,
    DISTANCE_FOR_POTENTIAL_MIGRATION,
    FORGETTING_TIME_DAYS,
    GLOBAL_SYSTEM_REGION,
    GLOBAL_TIME_ZONE,
    MIGRATION_COST_ESTIMATE,
    MINIMAL_SOLVE_THRESHOLD,
    SOLVER_INPUT_GRID_CARBON_DEFAULT,
    STOCHASTIC_HEURISTIC_DEPLOYMENT_ALGORITHM_CARBON_PER_INSTANCE_INVOCATION_ESTIMATE,
    TIME_FORMAT,
    TIME_FORMAT_DAYS,
    WORKFLOW_INSTANCE_TABLE,
)
from caribou.data_collector.components.workflow.workflow_collector import WorkflowCollector
from caribou.deployment_solver.deployment_algorithms.coarse_grained_deployment_algorithm import (
    CoarseGrainedDeploymentAlgorithm,
)
from caribou.deployment_solver.deployment_algorithms.deployment_algorithm import DeploymentAlgorithm
from caribou.deployment_solver.deployment_algorithms.fine_grained_deployment_algorithm import (
    FineGrainedDeploymentAlgorithm,
)
from caribou.deployment_solver.deployment_algorithms.stochastic_heuristic_deployment_algorithm import (
    StochasticHeuristicDeploymentAlgorithm,
)
from caribou.deployment_solver.workflow_config import WorkflowConfig
from caribou.monitors.monitor import Monitor

deployment_algorithm_mapping = {
    "coarse_grained_deployment_algorithm": CoarseGrainedDeploymentAlgorithm,
    "fine_grained_deployment_algorithm": FineGrainedDeploymentAlgorithm,
    "stochastic_heuristic_deployment_algorithm": StochasticHeuristicDeploymentAlgorithm,
}


class DeploymentManager(Monitor):
    def __init__(self) -> None:
        super().__init__()
        self.workflow_collector = WorkflowCollector()

    def check(self) -> None:
        deployment_manager_client = self._endpoints.get_deployment_manager_client()
        workflow_ids = deployment_manager_client.get_keys(DEPLOYMENT_MANAGER_RESOURCE_TABLE)
        print(workflow_ids)
        data_collector_client = self._endpoints.get_data_collector_client()

        for workflow_id in workflow_ids:
            workflow_info_raw = deployment_manager_client.get_value_from_table(
                DEPLOYMENT_MANAGER_WORKFLOW_INFO_TABLE, workflow_id
            )

            if workflow_info_raw is None or workflow_info_raw == "":
                workflow_info = None
            else:
                workflow_info = json.loads(workflow_info_raw)
                current_time = datetime.now(GLOBAL_TIME_ZONE)
                next_check = datetime.strptime(workflow_info["next_check"], TIME_FORMAT)
                if current_time < next_check:
                    pass
                    # continue

            self.workflow_collector.run_on_workflow(workflow_id)

            workflow_config_from_table = data_collector_client.get_value_from_table(
                DEPLOYMENT_MANAGER_RESOURCE_TABLE, workflow_id
            )

            workflow_json = json.loads(workflow_config_from_table)

            if "workflow_config" not in workflow_json:
                raise ValueError("Invalid workflow config")

            workflow_config_dict = json.loads(workflow_json["workflow_config"])

            print(f'Workflow name: {workflow_config_dict.get('workflow_name', None)}')

            if workflow_config_dict.get('workflow_name', None) != 'map_reduce':
                continue

            workflow_config = WorkflowConfig(workflow_config_dict)

            workflow_summary_raw = data_collector_client.get_value_from_table(WORKFLOW_INSTANCE_TABLE, workflow_id)

            workflow_summary = json.loads(workflow_summary_raw)

            last_solved = self._get_last_solved(workflow_info)

            total_invocation_counts_since_last_solved = self._get_total_invocation_counts_since_last_solved(
                workflow_summary, last_solved
            )

            # The solver has never been run before for this workflow, and the workflow has not been invoked enough
            # collect more data and wait
            if total_invocation_counts_since_last_solved < MINIMAL_SOLVE_THRESHOLD and workflow_info is None:
                pass
                # continue

            # Income token
            positive_carbon_savings_token = self._calculate_positive_carbon_savings_token(
                workflow_config.home_region, workflow_summary, total_invocation_counts_since_last_solved
            )

            carbon_budget_overflow_last_solved = (
                workflow_info["tokens_left"] if (workflow_info and "tokens_left" in workflow_info) else 0
            )

            affordable_deployment_algorithm_run = self._calculate_affordable_deployment_algorithm_run(
                len(workflow_config.instances), positive_carbon_savings_token + carbon_budget_overflow_last_solved
            )

            if not affordable_deployment_algorithm_run:
                pass
                # carbon_cost = self._get_cost(len(workflow_config.instances))
                # self._update_workflow_info(
                #     carbon_cost - positive_carbon_savings_token - carbon_budget_overflow_last_solved, workflow_id
                # )
                # continue

            expiry_delta_seconds = self._upload_new_workflow_info(
                affordable_deployment_algorithm_run["leftover_tokens"], workflow_id
            )

            solve_hours = self._get_solve_hours(affordable_deployment_algorithm_run["number_of_solves"])
            pdb.set_trace()
            self._run_deployment_algorithm(workflow_config, solve_hours, expiry_delta_seconds)

    def _update_workflow_info(self, token_missing: int, workflow_id: str) -> None:
        next_solve_delta_scale = self._get_sigmoid_scale(token_missing)
        new_workflow_info = {
            "next_check": (
                datetime.now(GLOBAL_TIME_ZONE)
                + timedelta(seconds=int(DEFAULT_MONITOR_COOLDOWN * next_solve_delta_scale))
            ).strftime(TIME_FORMAT)
        }

        self._endpoints.get_deployment_manager_client().set_value_in_table(
            DEPLOYMENT_MANAGER_WORKFLOW_INFO_TABLE, workflow_id, json.dumps(new_workflow_info)
        )

    def _upload_new_workflow_info(self, tokens_left: int, workflow_id: str) -> int:
        next_solve_delta_scale = self._get_sigmoid_scale(tokens_left)
        next_solve_delta = int(DEFAULT_MONITOR_COOLDOWN * next_solve_delta_scale)
        new_workflow_info = {
            "last_solved": datetime.now(GLOBAL_TIME_ZONE).strftime(TIME_FORMAT),
            "tokens_left": tokens_left,
            "next_check": (datetime.now(GLOBAL_TIME_ZONE) + timedelta(seconds=next_solve_delta)).strftime(TIME_FORMAT),
        }

        self._endpoints.get_deployment_manager_client().set_value_in_table(
            DEPLOYMENT_MANAGER_WORKFLOW_INFO_TABLE, workflow_id, json.dumps(new_workflow_info)
        )

        return next_solve_delta

    def _run_deployment_algorithm(
        self,
        workflow_config: WorkflowConfig,
        solve_hours: Optional[list[str]] = None,
        expiry_delta_seconds: int = DEFAULT_MONITOR_COOLDOWN,
    ) -> None:
        # TODO (#178): Instead of directly calling add message to solver queue (low-priority right now)
        deployment_algorithm_class = deployment_algorithm_mapping.get(workflow_config.deployment_algorithm)
        if deployment_algorithm_class:
            deployment_algorithm: DeploymentAlgorithm = deployment_algorithm_class(workflow_config, expiry_delta_seconds)  # type: ignore
            deployment_algorithm.run(solve_hours)
        else:
            raise ValueError("Invalid deployment algorithm")

    def _get_sigmoid_scale(self, x: float) -> float:
        return 3 / (1 + np.exp(-0.02 * x)) - 1

    def _get_last_solved(self, workflow_info: Optional[dict]) -> datetime:
        if workflow_info is None or "last_solved" not in workflow_info:
            last_solved = datetime.now(GLOBAL_TIME_ZONE) - timedelta(days=FORGETTING_TIME_DAYS)
        else:
            last_solved = datetime.strptime(workflow_info["last_solved"], TIME_FORMAT)

        return last_solved

    def _get_total_invocation_counts_since_last_solved(self, workflow_summary: dict, last_solved: datetime) -> int:
        if "daily_invocation_counts" not in workflow_summary:
            return 0

        total_invocation_counts_since_last_sync = 0
        for day in workflow_summary["daily_invocation_counts"]:
            if datetime.strptime(day, TIME_FORMAT_DAYS) > last_solved:
                total_invocation_counts_since_last_sync += workflow_summary["daily_invocation_counts"][day]

        return total_invocation_counts_since_last_sync

    def _calculate_positive_carbon_savings_token(
        self, home_region: str, workflow_summary: dict, total_invocation_counts_since_last_solved: int
    ) -> int:
        potential_carbon_savings_per_invocation_s = self._get_potential_carbon_savings_per_invocation_s(home_region)
        runtime = self._get_runtime_avg(workflow_summary)
        return int(
            math.ceil(potential_carbon_savings_per_invocation_s * runtime * total_invocation_counts_since_last_solved)
        )

    def _get_potential_carbon_savings_per_invocation_s(self, home_region: str) -> float:
        home_region_carbon_info_raw = self._endpoints.get_deployment_manager_client().get_value_from_table(
            CARBON_REGION_TABLE, home_region
        )
        if home_region_carbon_info_raw is None:
            raise ValueError("Invalid home region carbon info")
        home_region_carbon_info = json.loads(home_region_carbon_info_raw)

        potential_offloading_regions = []

        for region, distance in home_region_carbon_info["transmission_distances"].items():
            if distance <= DISTANCE_FOR_POTENTIAL_MIGRATION:
                potential_offloading_regions.append(region)

        carbon_intensities = []
        for region in potential_offloading_regions:
            region_carbon_raw = self._endpoints.get_deployment_manager_client().get_value_from_table(
                CARBON_REGION_TABLE, region
            )
            if region_carbon_raw is None:
                region_carbon_intensity = SOLVER_INPUT_GRID_CARBON_DEFAULT
            else:
                region_carbon_intensity = json.loads(region_carbon_raw)["averages"]["overall"]["carbon_intensity"]

            if region_carbon_intensity == SOLVER_INPUT_GRID_CARBON_DEFAULT:
                # Filter out regions with no carbon intensity data since they skew the std
                continue

            carbon_intensities.append(region_carbon_intensity)

        return float(np.std(carbon_intensities) * CARBON_INTENSITY_TO_INVOCATION_SECOND_ESTIMATE)

    def _get_runtime_avg(self, workflow_summary: dict) -> float:
        return np.array(workflow_summary["workflow_runtime_samples"]).mean()

    def _calculate_affordable_deployment_algorithm_run(
        self, number_of_instances: int, token_budget: int
    ) -> Optional[dict]:
        algorithm_estimates = {
            "coarse_grained_deployment_algorithm": (
                COARSE_GRAINED_DEPLOYMENT_ALGORITHM_CARBON_PER_INSTANCE_INVOCATION_ESTIMATE
            ),
            "stochastic_heuristic_deployment_algorithm": (
                STOCHASTIC_HEURISTIC_DEPLOYMENT_ALGORITHM_CARBON_PER_INSTANCE_INVOCATION_ESTIMATE
            ),
        }

        current_best_affordable_deployment_algorithm_run = None
        number_of_solves = [1, 2, 3, 4, 6, 8, 12, 24]
        for algorithm_name, algorithm_estimate in algorithm_estimates.items():
            for number_of_solve in number_of_solves:
                carbon_cost = self._get_cost(number_of_instances, number_of_solve, algorithm_estimate)
                if carbon_cost < token_budget:
                    current_best_affordable_deployment_algorithm_run = {
                        "number_of_solves": number_of_solve,
                        "algorithm": algorithm_name,
                        "leftover_tokens": token_budget - carbon_cost,
                    }
                else:
                    return current_best_affordable_deployment_algorithm_run

        return current_best_affordable_deployment_algorithm_run

    def _get_cost(
        self,
        number_of_instances: int,
        number_of_solves: int = 1,
        algorithm_estimate: float = COARSE_GRAINED_DEPLOYMENT_ALGORITHM_CARBON_PER_INSTANCE_INVOCATION_ESTIMATE,
    ) -> int:
        carbon_intensity_system = self._get_carbon_intensity_system()
        return int(
            number_of_instances * number_of_solves * algorithm_estimate * carbon_intensity_system
            + number_of_instances * MIGRATION_COST_ESTIMATE
        )

    def _get_carbon_intensity_system(self) -> float:
        region_carbon_raw = self._endpoints.get_deployment_manager_client().get_value_from_table(
            CARBON_REGION_TABLE, f"aws:{GLOBAL_SYSTEM_REGION}"
        )

        if region_carbon_raw is None:
            raise ValueError("Invalid system region carbon info")

        region_carbon_intensity = json.loads(region_carbon_raw)["averages"]["overall"]["carbon_intensity"]
        return region_carbon_intensity

    def _get_solve_hours(self, number_of_solves: int) -> list[str]:
        # Calculate the interval between solves
        interval = 24 // number_of_solves

        # Calculate the solve hours
        solve_hours = [str(hour) for hour in range(0, 24, interval)]

        return solve_hours
