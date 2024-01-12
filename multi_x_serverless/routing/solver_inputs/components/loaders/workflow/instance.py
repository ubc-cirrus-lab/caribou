import numpy as np

from ..loader import Loader


class WorkflowInstanceLoader(Loader):
    def __init__(self):
        super().__init__()

    def setup(self, workflow_ID: str) -> bool:
        self._data = {}

        # TODO: Load data from database, convert to proper format and store in self._data

        # template of output data
        self._data = {
            "execution_time": {},  # "PLACEHOLDER: loaded dictionary (instance_name: execution time value in seconds)",
        }

        return False  # Not implemented
