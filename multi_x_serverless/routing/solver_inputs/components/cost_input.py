from .input import Input

from multi_x_serverless.routing.workflow_config import WorkflowConfig

from multi_x_serverless.routing.models.indexer import Indexer

import numpy as np

class CostInput(Input):
    def __init__(self):
        super().__init__()

    def setup(self, regions: np.ndarray, config: WorkflowConfig, regions_indexer: Indexer, instance_indexer: Indexer) -> None:
        super().setup()
    
    def get_transmission_value(self, from_instance_index: int, to_instance_index: int, from_region_index: int, to_region_index: int) -> float:
        return 0.0