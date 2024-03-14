import json
import math
import numpy as np
from typing import Tuple
from multi_x_serverless.common.constants import (
    SOLVER_UPDATE_CHECKER_RESOURCE_TABLE, 
    WORKFLOW_SUMMARY_TABLE, 
    SOLVER_UPDATE_CHECKER_WORKFLOW_INFO_TABLE, 
    CARBON_REGION_TABLE,
    WORKFLOW_INSTANCE_TABLE
)
from multi_x_serverless.routing.solver.bfs_fine_grained_solver import BFSFineGrainedSolver
from multi_x_serverless.routing.solver.coarse_grained_solver import CoarseGrainedSolver
from multi_x_serverless.routing.solver.stochastic_heuristic_descent_solver import StochasticHeuristicDescentSolver
from multi_x_serverless.routing.workflow_config import WorkflowConfig
from multi_x_serverless.update_checkers.update_checker import UpdateChecker
from multi_x_serverless.data_collector.components.workflow.workflow_collector import WorkflowCollector

class SolverUpdateChecker(UpdateChecker):
    def __init__(self) -> None:
        super().__init__("solver_update_checker")

    def check(self) -> None:
        # get workflow ids from SOLVER_UPDATE_CHECKER_RESOURCE_TABLE
        solver_update_checker_client = self._endpoints.get_solver_update_checker_client()
        workflow_ids = solver_update_checker_client.get_keys(SOLVER_UPDATE_CHECKER_RESOURCE_TABLE)
        data_collector_client = self._endpoints.get_data_collector_client()

        deployment_algorithm_mapping = {
            "coarse_grained_deployment_algorithm": CoarseGrainedDeploymentAlgorithm,
            "fine_grained_deployment_algorithm": FineGrainedDeploymentAlgorithm,
            "stochastic_heuristic_deployment_algorithm": StochasticHeuristicDeploymentAlgorithm,
        }

        for workflow_id in workflow_ids:

            # I assume the workflow_info_table will have the format of:
            # SOLVER_UPDATE_CHECKER_WORKFLOW_INFO_TABLE = {
            #     workflow_id_1: {
            #         "last_solved": "2021-10-01T00:00:00Z",
            #         "frequency": "12",
            #         "tokens_left": 100,
            #         "expiry_date": "2022-10-01T00:00:00Z",
            #     },
            #     workflow_id_2: {
            #         "last_solved": "2021-10-01T00:00:00Z",
            #         "frequency": "1",
            #         "tokens_left": 100,
            #         "expiry_date": "2022-10-01T00:00:00Z",
            #     }
            # }
            # if the workflow_id does not exist in the table, it means it have not been seen before
            # assumed that we run the update solver bi-weekly

            workflow_infos = solver_update_checker_client.get_value_from_table(SOLVER_UPDATE_CHECKER_WORKFLOW_INFO_TABLE, workflow_id)

            workflow_config_from_table = data_collector_client.get_value_from_table(
                SOLVER_UPDATE_CHECKER_RESOURCE_TABLE, workflow_id
            )

            # start hop contains the home region of the workflow

            workflow_json = json.loads(workflow_config_from_table)

            if "workflow_config" not in workflow_json:
                raise ValueError("Invalid workflow config")

            workflow_config_dict = json.loads(workflow_json["workflow_config"])
            
            # pass to solver
            workflow_config = WorkflowConfig(workflow_config_dict)

            # TODO (#128): Implement adaptive and stateful update checker
            # update workflow_config, then write back to SOLVER_UPDATE_CHECKER_RESOURCE_TABLE as string (to_json)

            workflow_summary = data_collector_client.get_last_value_from_sort_key_table(
                WORKFLOW_SUMMARY_TABLE, workflow_id
            )

            if len(workflow_summary) == 0:
                raise ValueError("Invalid workflow summary")

            workflow_summary_json = json.loads(workflow_summary[1])

            time_since_last_sync = workflow_summary_json["time_since_last_sync"]
            total_invocations = workflow_summary_json["total_invocations"]

            # Extrapoloate the number of invocations per month
            months_between_summary = time_since_last_sync / (60 * 60 * 24 * 30)
            
            # std carbon intensity calculation
            carbon_info = solver_update_checker_client.get_value_from_table(CARBON_REGION_TABLE, workflow_config.home_region)
            if carbon_info is None:
                raise ValueError("Invalid carbon region table")
            carbon_info_json = json.loads(carbon_info)

            std_carbon_intensity = self.calculate_std_carbon_intensity(carbon_info_json, solver_update_checker_client)
            
            # runtime/number of invocation calculation
            runtime_info = solver_update_checker_client.get_value_from_table(WORKFLOW_INSTANCE_TABLE, workflow_id)
            if runtime_info is None:
                raise ValueError("Invalid runtime info table")
            runtime_info_json = json.loads(runtime_info)

            runtime, num_invocations = self.calculate_runtime_and_invocations(runtime_info_json)

            # Income token
            estimated_carbon_saving = std_carbon_intensity * runtime * num_invocations
            
            # Cost token
            # TODO: implement cost token
            estimated_overhead = 10

            if workflow_infos is None:
                # When the Update Checker has not been run before for this workflow
                # Run the workflow collector and solver
                WorkflowCollector().collect_single_workflow(workflow_id)

                for instance in workflow_summary_json["instance_summary"]:
                    # If the invocation count is not sufficient to form an distribution, we do nothin
                    if instance["invocation_count"] < 10:
                        continue
                
                # Run the workflow collector and solver
                solver_class = solver_mapping.get(workflow_config.solver)
                if solver_class:
                    solver = solver_class(workflow_config)
                    solver.solve()
                else:
                    # we should never reach here
                    raise ValueError("Invalid solver name")
                
            else:
                
                # When the Update Checker has been run before for this workflow
                WorkflowCollector().collect_single_workflow(workflow_id)



        
    def sigmoid(x: float) -> float:
        return 1 / (1 + math.exp(-x))
    
    def calculate_std_carbon_intensity(carbon_info, solver_update_checker_client) -> float:
        regions_over_threshold = []

        for region, distance in carbon_info["transmission_distances"].items():
            if distance > 150:
                regions_over_threshold.append(region)
        
        overall_average = []

        for region in regions_over_threshold:
            region_overall_average = json.loads(solver_update_checker_client.get_value_from_table(CARBON_REGION_TABLE, region))["averages"].overall
            overall_average.append(region_overall_average)

        return np.std(overall_average)
    
    def calculate_runtime_and_invocations(runtime_info) -> Tuple[float, float]:
        # TODO: implement invocation count
        num_invocations = 10

        return np.array(runtime_info.workflow_runtime_samples).mean(), num_invocations
