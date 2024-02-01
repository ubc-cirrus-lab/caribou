from multi_x_serverless.update_checkers.update_checker import UpdateChecker
from multi_x_serverless.common.constants import WORKFLOW_PLACEMENT_SOLVER_STAGING_AREA_TABLE

class DeploymentUpdateChecker(UpdateChecker):
    def __init__(self, name):
        super().__init__(name)

    def check(self) -> None:
        updated_workflow_placements = self._endpoints.get_solver_workflow_placement_decision_client().get_keys(WORKFLOW_PLACEMENT_SOLVER_STAGING_AREA_TABLE)
        if len(updated_workflow_placements) > 0:
            print(f"Found {len(updated_workflow_placements)} updated workflow placements")
            return False
        else:
            pass
