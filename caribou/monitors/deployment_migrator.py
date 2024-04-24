import logging

from caribou.common.constants import WORKFLOW_PLACEMENT_SOLVER_STAGING_AREA_TABLE
from caribou.deployment.server.re_deployment_server import ReDeploymentServer
from caribou.monitors.monitor import Monitor

logger = logging.getLogger(__name__)


class DeploymentMigrator(Monitor):
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
