import numpy as np

from multi_x_serverless.routing.current.workflow_config import WorkflowConfig


class Region:
    # TODO (#15): Implement this class
    def __init__(self, workflow_config: WorkflowConfig) -> None:
        self._workflow_config = workflow_config

    def get_all_regions(self) -> np.ndarray:
        # TODO (#15): Implement this function
        return np.array([])
