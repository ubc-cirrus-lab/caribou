import numpy as np

from multi_x_serverless.routing.solver_inputs.components.loaders.loader import Loader


class WorkflowInstanceFromToLoader(Loader):
    def setup(self, workflow_id: str) -> bool:
        self._data = {}

        # TODO (#35): Load data from database, convert to proper format and store in self._data

        # TODO (#35): When loading we need to pay attention that JSON keys are strings and not tuples
        # The tuples have to be converted from the strings in the JSON string loaded from the table

        # Template of the loaded data - Also in docs
        # Key for this data is the (from instance name, to instance name)
        self._data = {
            "data_transfer_size": {("i1", "i2"): 0.4},  # Data transfer size in GB
            "probability": {("i1", "i2"): 0.95},  # Probability of instance being called in fractions
        }

        return False  #  Not yet implemented
