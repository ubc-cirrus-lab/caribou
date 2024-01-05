from abc import ABC, abstractmethod

from multi_x_serverless.routing.workflow_config import WorkflowConfig

from multi_x_serverless.routing.models.indexer import Indexer

from multi_x_serverless.routing.data_sources.components.source import Source
from multi_x_serverless.routing.data_sources.components.region_specific.carbon import CarbonSource
from multi_x_serverless.routing.data_sources.components.region_specific.cost import CostSource
from multi_x_serverless.routing.data_sources.components.runtime import RuntimeSource
from multi_x_serverless.routing.data_sources.components.instance_specific.data_transfer import DataTransferSource

import numpy as np

class DataManager():
    def __init__(self, config: WorkflowConfig, regions_indexer: Indexer, instance_indexer: Indexer):
        self._config = config
        self._regions_indexer = regions_indexer
        self._instance_indexer = instance_indexer

        self._carbon_source = CarbonSource()
        self._cost_source = CostSource()
        self._runtime_source = RuntimeSource()
        self._file_source = DataTransferSource()

    def setup(self, regions: np.ndarray):
        # Handle parsing common information to pass to the data sources
        workflow_name = self._config['workflow_name']
        workflow_internal_id = self._config['workflow_internal_id']

        # Clear cache And perform data aquisition (This will setup and cache the matrix)
        self._carbon_source.setup(regions, self._config, self._regions_indexer, self._instance_indexer)
        self._cost_source.setup(regions, self._config, self._regions_indexer, self._instance_indexer)
        self._runtime_source.setup(regions, self._config, self._regions_indexer, self._instance_indexer)
        self._file_source.setup(regions, self._config, self._regions_indexer, self._instance_indexer)

    def get_execution_matrix(self, desired_source: str) -> np.ndarray:
        return self._get_source(desired_source).get_execution_matrix()

    def get_transmission_matrix(self, desired_source: str) -> np.ndarray:
        return self._get_source(desired_source).get_transmission_matrix()
    
    def _get_source(self, desired_source: str) -> Source:
        return {
            "Carbon": self._carbon_source,
            "Cost": self._cost_source,
            "Runtime": self._runtime_source,
            "File": self._file_source,
        }[desired_source]