# Inner Components

# Loader
from .components.loaders.loader_manager import LoaderManager

# Data sources
from .components.data_sources.data_source_manager import DataSourceManager
from .components.data_sources.source import Source

# Inputs
from .components.input import Input
from .components.carbon_input import CarbonInput
from .components.cost_input import CostInput
from .components.runtime_input import RuntimeInput

# Outside library
from multi_x_serverless.routing.workflow_config import WorkflowConfig
from multi_x_serverless.routing.models.indexer import Indexer

import numpy as np

class InputManager():
    def __init__(self, config: WorkflowConfig, regions_indexer: Indexer, instance_indexer: Indexer):
        super().__init__()
        self._config = config
        self._regions_indexer = regions_indexer
        self._instance_indexer = instance_indexer

        # initialize components
        # Loader Manager
        self._loader_manager = LoaderManager()

        # Data sources
        self._data_source_manager = DataSourceManager()

        # Finalized inputs
        self._carbon_input = CarbonInput()
        self._cost_input = CostInput()
        self._runtime_input = RuntimeInput()

    def setup(self, regions: np.ndarray) -> (bool, str):
        # Utilize the Loaders to load the data from the database

        # Workflow loaders use the workfload unique ID from the config
        workflow_ID = self._config.get("workflow_ID", None)
        if workflow_ID is None:
            return False, "Workflow ID not found in config"
        
        if (not self._loader_manager.setup(regions, workflow_ID)[0]):
            return False, "Failed one or more loaders has failed to load data"
        
        all_loaded_informations = self._loader_manager.retrieve_data()

        # Using those information, we can now setup the data sources
        self._workflow_instance_input.setup(workflow_instance_information, workflow_instance_from_to_information)
        self._datacenter_region_input.setup(datacenter_region_information, datacenter_region_from_to_information)
        self._carbon_region_input.setup(carbon_region_information, carbon_region_from_to_information)

        # Now take the loaded data and send it to the data sources, which will be used in the component input managers
        self._carbon_input.setup(regions, self._config, self._regions_indexer, self._instance_indexer)
        self._cost_input.setup(regions, self._config, self._regions_indexer, self._instance_indexer)
        self._runtime_input.setup(regions, self._config, self._regions_indexer, self._instance_indexer)

    def get_execution_value(self, desired_calculator: str, instance_index: int, region_index: int) -> float:
        return self._get_input_component_manager(desired_calculator).get_execution_value(instance_index, region_index)

    def get_execution_cost_carbon_runtime(self, instance_index: int, region_index: int) -> float:
        results = []
        calculators = ["Cost", "Carbon", "Runtime"]
        for calculator in calculators:
            results.append(self._get_input_component_manager(calculator).get_execution_value(instance_index, region_index))
        return results

    def get_transmission(self, desired_calculator: str, from_instance_index: int, to_instance_index: int, from_region_index: int, to_region_index: int) -> list[float]:
        return self._get_input_component_manager(desired_calculator).get_transmission_value(from_instance_index, to_instance_index, from_region_index, to_region_index)
    
    def get_transmission_cost_carbon_runtime(self, from_instance_index: int, to_instance_index: int, from_region_index: int, to_region_index: int) -> list[float]:
        results = []
        calculators = ["Cost", "Carbon", "Runtime"]
        for calculator in calculators:
            results.append(self._get_input_component_manager(calculator).get_transmission_value(from_instance_index, to_instance_index, from_region_index, to_region_index))
        return results

    def _get_input_component_manager(self, desired_calculator: str) -> Input:
        return {
            "Carbon": self._carbon_input,
            "Cost": self._cost_input,
            "Runtime": self._runtime_input,
        }[desired_calculator]