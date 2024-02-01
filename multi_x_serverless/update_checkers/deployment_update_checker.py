from multi_x_serverless.common.constants import WORKFLOW_PLACEMENT_SOLVER_STAGING_AREA_TABLE
from multi_x_serverless.deployment.server.main import run
from multi_x_serverless.update_checkers.update_checker import UpdateChecker


class DeploymentUpdateChecker(UpdateChecker):
    # This should be a timed update checker (every hour)
    def __init__(self, name):
        super().__init__(name)

    def check(self) -> None:
        updated_workflow_placements = self._endpoints.get_solver_workflow_placement_decision_client().get_keys(
            WORKFLOW_PLACEMENT_SOLVER_STAGING_AREA_TABLE
        )
        for updated_workflow_placement in updated_workflow_placements:
            run(updated_workflow_placement)
