# Inner Components

# Loader
from .components.loaders.loader_manager import LoaderManager
from .components.loaders.region_viability_loader import RegionViabilityLoader

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
    def __init__(self, config: WorkflowConfig):
        super().__init__()
        self._config = config

        # initialize components
        # Loader Manager
        self._loader_manager = LoaderManager()

        # Viability loader
        self._region_viability_loader = RegionViabilityLoader()

        # Data sources
        self._data_source_manager = DataSourceManager()

        # Finalized inputs
        self._carbon_input = CarbonInput()
        self._cost_input = CostInput()
        self._runtime_input = RuntimeInput()
    
    def setup(self, regions_indexer: Indexer, instance_indexer: Indexer) -> bool:
        # Utilize the Loaders to load the data from the database

        # Workflow loaders use the workfload unique ID from the config
        workflow_ID = self._config.get("workflow_ID", None)
        if workflow_ID is None:
            return False, "Workflow ID not found in config"
        
        regions = regions_indexer.get_value_indices.keys()
        success = self._loader_manager.setup(regions, workflow_ID)
        if (not success):
            # return False, "Failed one or more loaders has failed to load data", 
            print("Failed one or more loaders has failed to load data")
            return False, 

        # Get the retrieved information
        all_loaded_informations = self._loader_manager.retrieve_data()

        # Using those information, we can now setup the data sources
        self._data_source_manager.setup(all_loaded_informations, regions_indexer, instance_indexer)

        # Now take the loaded data and send it to the data sources, which will be used in the component input managers
        instances_indicies = instance_indexer.get_value_indices.values()
        regions_indicies = regions_indexer.get_value_indices.values()
        self._carbon_input.setup(instances_indicies, regions_indicies, self._data_source_manager)
        self._cost_input.setup(instances_indicies, regions_indicies, self._data_source_manager)
        self._runtime_input.setup(instances_indicies, regions_indicies, self._data_source_manager)

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
    
    def get_all_regions(self) -> list[dict]:
        all_regions = self._region_viability_loader.retrieve_data()
        if (all_regions is None):
            self._region_viability_loader.setup()
        
        return self._region_viability_loader.retrieve_data()

    def _get_input_component_manager(self, desired_calculator: str) -> Input:
        return {
            "Carbon": self._carbon_input,
            "Cost": self._cost_input,
            "Runtime": self._runtime_input,
        }[desired_calculator]