from .input import Input

from multi_x_serverless.routing.workflow_config import WorkflowConfig

from multi_x_serverless.routing.models.indexer import Indexer

import numpy as np

class CarbonInput(Input):
    def __init__(self):
        super().__init__()
    
    def setup(self, regions: np.ndarray, regions_indexer: Indexer) -> None:
        super().setup()

        # This is where we will load the data from the databases
        
        # This is the carbon intensity of each region (By grid)
        carbon_intensity_information = self.load_database("carbon_intensity_tranmission", regions)

        # This is the carbon intensity of each region (By grid)
        carbon_intensity_datacenters = self.load_database("carbon_intensity_execution", regions)

        # Time to now setup both the execution and transmission matrices

        # For transmission, this denotes the amount of carbon emitted per GB of data transmitted (In gCO2e/GB)
        # between 2 datacenter regions.
        self._transmission_matrix = np.zeros((len(regions), len(regions)), dtype=np.float32) # Default to big values
        for from_region in regions:
            from_region_index = regions_indexer.get_index(from_region)
            for to_region in regions:
                to_region_index = regions_indexer.get_index(to_region)
                self._transmission_matrix[from_region_index][to_region_index] = carbon_intensity_information.get(from_region, {}).get(to_region, 1000) # Default to huge carbon value if not present

        # For execution, this denotes the carbon emitted for execution in a datacenter per kWh of power consumed. (In gCO2e/kWh) -> include PUE and (optionally CFE)
        self._execution_matrix = np.zeros((len(regions)), dtype=np.float32)
        for region in regions:
            region_index = regions_indexer.get_index(region)
            self._execution_matrix[region_index] = carbon_intensity_datacenters.get(region, 1000)

    def load_database(self, database_name: str, regions: np.ndarray) -> dict:
        #TODO (#35): Implement this function
        return None