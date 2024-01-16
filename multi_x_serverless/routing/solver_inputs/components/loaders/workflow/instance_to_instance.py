import numpy as np

from multi_x_serverless.routing.solver_inputs.components.loaders.loader import Loader


class WorkflowInstanceFromToLoader(Loader):
    def setup(self, workflow_id: str) -> bool:
        self._data = {}

        # TODO (#35): Load data from database, convert to proper format and store in self._data

        # Template of the loaded data - Also in docs
        # Key for this data is the (from instance name, to instance name)
        self._data = {
            "data_transfer_size": {("i1", "i2"): 0.4},  # Data transfer size in GB
        }

        return False  #  Not yet implemented
