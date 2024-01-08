from ..source import Source

from multi_x_serverless.routing.workflow_config import WorkflowConfig
from multi_x_serverless.routing.models.indexer import Indexer

import numpy as np

class RuntimeSource(Source):
    def __init__(self):
        super().__init__()

    def setup(self, regions: np.ndarray, config: WorkflowConfig, regions_indexer: Indexer, instance_indexer: Indexer) -> None:
        super().setup()

        # Time to now setup both the execution and transmission matrices
        # TODO (#15): Implement this function
        self._execution_matrix = np.zeros((len(regions), len(regions)))



        
        self._transmission_matrix = np.zeros((len(regions), len(regions)))

    def load_database(self) -> dict:
        #TODO (#35): Implement this function
        return None