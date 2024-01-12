from ..loader import Loader

import numpy as np

class WorkflowInstanceFromToLoader(Loader):
    def __init__(self):
        super().__init__()
    
    def setup(self, workflow_ID: str) -> bool: # Returns if successful and reason if not
        self._data = {}

        # TODO: Load data from database, convert to proper format and store in self._data

        # template of output data
        self._data = {
            "data_transfer_size": {}, # "PLACEHOLDER: loaded dictionary ((from_instance_name, to_instance_name): data transfer size in GB)",
        }

        return False # Not implemented