import logging

from multi_x_serverless.common.constants import WORKFLOW_PLACEMENT_SOLVER_STAGING_AREA_TABLE
from multi_x_serverless.deployment.server.re_deployment_server import ReDeploymentServer
from multi_x_serverless.update_checkers.update_checker import UpdateChecker

logger = logging.getLogger(__name__)


class DeploymentUpdateChecker(UpdateChecker):
    def __init__(self) -> None:
        super().__init__("deployment_update_checker")

    # This should be a timed update checker (every hour)
    def check(self) -> None:
        updated_workflow_placements = (
            self._endpoints.get_deployment_algorithm_workflow_placement_decision_client().get_keys(
                WORKFLOW_PLACEMENT_SOLVER_STAGING_AREA_TABLE
            )
        )
        for workflow_id in updated_workflow_placements:
            logger.info(f"Checking if the deployment should be updated for workflow: {workflow_id}")
            re_deployment_server = ReDeploymentServer(workflow_id)
            re_deployment_server.run()
