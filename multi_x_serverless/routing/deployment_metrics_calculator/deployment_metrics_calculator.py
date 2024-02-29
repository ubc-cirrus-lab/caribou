from multi_x_serverless.routing.workflow_config import WorkflowConfig
from multi_x_serverless.routing.solver.input.input_manager import InputManager


class DeploymentMetricsCalculator:
    def __init__(self, workflow_config: WorkflowConfig, input_manager: InputManager):
        self.workflow_config = workflow_config
        self.input_manager = input_manager

    def calculate_deployment_metrics(self, deployment: list[int]) -> dict[str, float]:
        # TODO (#145): Implement new data model for solver
        # Expected output:
        # {
        #     "average_cost": 0.0,
        #     "average_runtime": 0.0,
        #     "average_carbon": 0.0,
        #     "tail_cost": 0.0,
        #     "tail_runtime": 0.0,
        #     "tail_carbon": 0.0,
        # }
        raise NotImplementedError
