import numpy as np

from multi_x_serverless.routing.solver_inputs.components.loaders.loader import Loader


class WorkflowInstanceLoader(Loader):
    def __init__(self):
        super().__init__()

    def setup(self, workflow_id: str) -> bool:
        self._data = {}

        # TODO (#35): Load data from database, convert to proper format and store in self._data

        # template of output data
        self._data = {
            "execution_time": {},  # "PLACEHOLDER: loaded dictionary (instance_name: execution time value in seconds)",
        }

        return False  # Not implemented
