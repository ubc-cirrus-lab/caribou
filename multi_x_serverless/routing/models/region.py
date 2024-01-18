from typing import Any

import numpy as np

from multi_x_serverless.routing.models.indexer import Indexer
from multi_x_serverless.routing.workflow_config import WorkflowConfig


class Region(Indexer):
    def __init__(self, regions: list[dict[str, str]]) -> None:
        self._value_indices: dict[tuple[str, str], int] = {
            (region["provider"], region["region"]): index for index, region in enumerate(regions)
        }
        super().__init__()
