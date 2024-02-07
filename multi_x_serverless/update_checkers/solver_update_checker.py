from multi_x_serverless.update_checkers.update_checker import UpdateChecker
from multi_x_serverless.common.constants import WORKFLOW_SUMMARY_TABLE, SOLVER_UPDATE_CHECKER_RESOURCE_TABLE
import json
from multi_x_serverless.routing.workflow_config import WorkflowConfig
from multi_x_serverless.routing.solver.coarse_grained_solver import CoarseGrainedSolver
from multi_x_serverless.routing.solver.bfs_fine_grained_solver import BFSFineGrainedSolver
from multi_x_serverless.routing.solver.stochastic_heuristic_descent_solver import StochasticHeuristicDescentSolver


class SolverUpdateChecker(UpdateChecker):
    def __init__(self) -> None:
        super().__init__("solver_update_checker")

    def check(self) -> None:
        # add which solver to use
        workflow_ids = self._endpoints.get_solver_update_checker_client().get_keys(SOLVER_UPDATE_CHECKER_RESOURCE_TABLE)
        data_collector_client = self._endpoints.get_data_collector_client()

        solver_mapping = {
            "coarse_grained_solver": CoarseGrainedSolver,
            "fine_grained_solver": BFSFineGrainedSolver,
            "stochastic_heuristic_solver": StochasticHeuristicDescentSolver,
        }

        for workflow_id in workflow_ids:
            workflow_config_from_table = data_collector_client.get_value_from_table(
                SOLVER_UPDATE_CHECKER_RESOURCE_TABLE, workflow_id
            )
            workflow_json = json.loads(workflow_config_from_table)
            # determines whether we should run solver
            workflow_summary = data_collector_client.get_value_from_table(WORKFLOW_SUMMARY_TABLE, workflow_id)
            # pass to solver
            workflow_config = WorkflowConfig(workflow_json)

            # TODO (#128): Implement adaptive and stateful update checker
            # update workflow_config, then write back to SOLVER_UPDATE_CHECKER_RESOURCE_TABLE as string (to_json)

            # check if solver should be run
            # very simple check for now, just check if the average invocations per month is greater than the threshold
            workflow_summary_json = json.loads(workflow_summary)
            months_between_summary = workflow_summary_json["months_between_summary"]
            total_invocations = workflow_summary_json["total_invocations"]

            if total_invocations / months_between_summary > workflow_config.num_calls_in_one_month:
                solver_class = solver_mapping.get(workflow_config.solver)
                if solver_class:
                    solver = solver_class(workflow_config)
                    solver.solve()
                else:
                    # we should never reach here
                    raise ValueError("Invalid solver name")
