import logging
import os

from caribou.common.constants import WORKFLOW_PLACEMENT_SOLVER_STAGING_AREA_TABLE
from caribou.deployment.server.re_deployment_server import ReDeploymentServer
from caribou.monitors.monitor import Monitor

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Only add a StreamHandler if not running in AWS Lambda
if "AWS_LAMBDA_FUNCTION_NAME" not in os.environ:
    if not logger.handlers:
        logger.addHandler(logging.StreamHandler())


class DeploymentMigrator(Monitor):
    def __init__(self, deployed_remotely: bool = False) -> None:
        super().__init__()
        self._deployed_remotely = deployed_remotely

    # This should be a timed update checker (every hour)
    def check(self) -> None:
        updated_workflow_placements = (
            self._endpoints.get_deployment_algorithm_workflow_placement_decision_client().get_keys(
                WORKFLOW_PLACEMENT_SOLVER_STAGING_AREA_TABLE
            )
        )
        for workflow_id in updated_workflow_placements:
            logger.info(f"Initializing redeploying workflow: {workflow_id}")
            if self._deployed_remotely:
                # Initiate on a separate lambda function
                self.remote_re_deploy_workflow(workflow_id)
            else:
                # Invoke locally/same lambda function
                self.re_deploy_workflow(workflow_id)

    def remote_re_deploy_workflow(self, workflow_id: str) -> None:
        framework_cli_remote_client = self._endpoints.get_framework_cli_remote_client()

        framework_cli_remote_client.invoke_remote_framework_internal_action(
            "re_deploy_workflow",
            {"workflow_id": workflow_id},
        )

    def re_deploy_workflow(self, workflow_id: str) -> None:
        re_deployment_server = ReDeploymentServer(workflow_id)
        re_deployment_server.run()
