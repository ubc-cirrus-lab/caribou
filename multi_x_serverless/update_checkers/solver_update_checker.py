import json
import logging

from multi_x_serverless.common.constants import SOLVER_UPDATE_CHECKER_RESOURCE_TABLE, WORKFLOW_SUMMARY_TABLE
from multi_x_serverless.routing.deployment_algorithms.coarse_grained_deployment_algorithm import (
    CoarseGrainedDeploymentAlgorithm,
)
from multi_x_serverless.routing.deployment_algorithms.deployment_algorithm import DeploymentAlgorithm
from multi_x_serverless.routing.deployment_algorithms.fine_grained_deployment_algorithm import (
    FineGrainedDeploymentAlgorithm,
)
from multi_x_serverless.routing.deployment_algorithms.stochastic_heuristic_deployment_algorithm import (
    StochasticHeuristicDeploymentAlgorithm,
)
from multi_x_serverless.routing.workflow_config import WorkflowConfig
from multi_x_serverless.update_checkers.update_checker import UpdateChecker

logger = logging.getLogger(__name__)


class SolverUpdateChecker(UpdateChecker):
    def __init__(self) -> None:
        super().__init__("solver_update_checker")

    def check(self) -> None:
        # add which solver to use
        workflow_ids = self._endpoints.get_deployment_algorithm_update_checker_client().get_keys(
            SOLVER_UPDATE_CHECKER_RESOURCE_TABLE
        )
        data_collector_client = self._endpoints.get_data_collector_client()

        deployment_algorithm_mapping = {
            "coarse_grained_deployment_algorithm": CoarseGrainedDeploymentAlgorithm,
            "fine_grained_deployment_algorithm": FineGrainedDeploymentAlgorithm,
            "stochastic_heuristic_deployment_algorithm": StochasticHeuristicDeploymentAlgorithm,
        }

        for workflow_id in workflow_ids:
            workflow_config_from_table = data_collector_client.get_value_from_table(
                SOLVER_UPDATE_CHECKER_RESOURCE_TABLE, workflow_id
            )
            workflow_json = json.loads(workflow_config_from_table)

            if "workflow_config" not in workflow_json:
                raise ValueError("Invalid workflow config")

            workflow_config_dict = json.loads(workflow_json["workflow_config"])
            # determines whether we should run deployment_algorithm
            workflow_summary = data_collector_client.get_value_from_table(WORKFLOW_SUMMARY_TABLE, workflow_id)

            if len(workflow_summary) == 0:
                raise ValueError("Invalid workflow summary")

            # pass to deployment_algorithm
            workflow_config = WorkflowConfig(workflow_config_dict)

            logger.info(f"Running deployment algorithm for workflow: {workflow_id}")
            deployment_algorithm: StochasticHeuristicDeploymentAlgorithm = StochasticHeuristicDeploymentAlgorithm(workflow_config)  # type: ignore
            deployment_algorithm.run(["0", "12", "23"])
