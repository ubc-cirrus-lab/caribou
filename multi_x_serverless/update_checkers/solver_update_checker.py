import json

from multi_x_serverless.common.constants import SOLVER_UPDATE_CHECKER_RESOURCE_TABLE, WORKFLOW_SUMMARY_TABLE, SOLVER_UPDATE_CHECKER_WORKFLOW_INFO_TABLE
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
        # add which solver to use

        # get workflow ids from SOLVER_UPDATE_CHECKER_RESOURCE_TABLE
        solver_update_checker_client = self._endpoints.get_solver_update_checker_client()
        workflow_ids = solver_update_checker_client.get_keys(SOLVER_UPDATE_CHECKER_RESOURCE_TABLE)
        data_collector_client = self._endpoints.get_data_collector_client()

        solver_mapping = {
            "coarse_grained_solver": CoarseGrainedSolver,
            "fine_grained_solver": BFSFineGrainedSolver,
            "stochastic_heuristic_solver": StochasticHeuristicDescentSolver,
        }

        for workflow_id in workflow_ids:

            # I assume the workflow_info_table will have the format of:
            # SOLVER_UPDATE_CHECKER_WORKFLOW_INFO_TABLE = {
            #     workflow_id_1: {
            #         "expired_times": 0,
            #         "frequency_per_run": 6,       # depends on the user preference on carbon/latency/cost can be integer between 1-24, assumed given by the user
            #     },
            #     workflow_id_2: {
            #         "expired_times": 1,
            #         "frequency_per_run": 1,
            #     }
            # }
            # if the workflow_id does not exist in the table, it means it have not been seen before
            # assumed that we run the update solver bi-weekly

            workflow_infos = solver_update_checker_client.get_value_from_table(SOLVER_UPDATE_CHECKER_WORKFLOW_INFO_TABLE, workflow_id)

            workflow_config_from_table = data_collector_client.get_value_from_table(
                SOLVER_UPDATE_CHECKER_RESOURCE_TABLE, workflow_id
            )
            workflow_json = json.loads(workflow_config_from_table)

            if "workflow_config" not in workflow_json:
                raise ValueError("Invalid workflow config")

            workflow_config_dict = json.loads(workflow_json["workflow_config"])
            # determines whether we should run solver
            workflow_summary = data_collector_client.get_last_value_from_sort_key_table(
                WORKFLOW_SUMMARY_TABLE, workflow_id
            )

            if len(workflow_summary) == 0:
                raise ValueError("Invalid workflow summary")

            # pass to solver
            workflow_config = WorkflowConfig(workflow_config_dict)

            # TODO (#128): Implement adaptive and stateful update checker
            # update workflow_config, then write back to SOLVER_UPDATE_CHECKER_RESOURCE_TABLE as string (to_json)

            workflow_summary_json = json.loads(workflow_summary[1])

            time_since_last_sync = workflow_summary_json["time_since_last_sync"]
            total_invocations = workflow_summary_json["total_invocations"]

            # Extrapoloate the number of invocations per month
            months_between_summary = time_since_last_sync / (60 * 60 * 24 * 30)

            if workflow_infos is None:
                # When the Update Checker has not been run before for this workflow
                # Run the workflow collector and solver

                for instance in workflow_summary_json["instance_summary"]:
                    # If the invocation count is not sufficient to form an distribution, we do nothin
                    if instance["invocation_count"] < 10:
                        continue

                # Run the workflow collector and solver
                WorkflowCollector().collect_single_workflow(workflow_id)
                solver_class = solver_mapping.get(workflow_config.solver)
                if solver_class:
                    solver = solver_class(workflow_config)
                    solver.solve()
                else:
                    # we should never reach here
                    raise ValueError("Invalid solver name")
                
            else:
                # After the Update checker has been run before for this workflow
                # Measured runtime per invocation * projected monthly invocation * std of all datacenters carbon intensity globally = approximate potential carbon saving of the workflow in the next month
                # If it's above the estimated overhead, run the workflow collector and solver.
                
                # TODO: ask about this
                carbon_saved_per_invocation = 1 

                carbon_saved_per_month = workflow_config.num_calls_in_one_month * carbon_saved_per_invocation
                

            # if total_invocations / months_between_summary > workflow_config.num_calls_in_one_month:
            #     solver_class = solver_mapping.get(workflow_config.solver)
            #     if solver_class:
            #         solver = solver_class(workflow_config)
            #         solver.solve()
            #     else:
            #         # we should never reach here
            #         raise ValueError("Invalid solver name")
