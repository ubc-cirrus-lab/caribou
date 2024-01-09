# Loaders
from .datacenter.region_to_region import DataCenterRegionToRegionLoader
from .datacenter.region import DataCenterRegionLoader
from .carbon.region_to_region import CarbonRegionFromToLoader
from .carbon.region import CarbonRegionLoader
from .workflow.instance_to_instance import WorkflowInstanceFromToLoader
from .workflow.instance import WorkflowInstanceLoader

# Others
import numpy as np

class LoaderManager():
    def __init__(self):
        super().__init__()

        # initialize components
        self._datacenter_region_to_region_loader = DataCenterRegionToRegionLoader()
        self._datacenter_region_loader = DataCenterRegionLoader()
        self._carbon_region_to_region_loader = CarbonRegionFromToLoader()
        self._carbon_region_loader = CarbonRegionLoader()
        self._workflow_instance_to_instance_loader = WorkflowInstanceFromToLoader()
        self._workflow_instance_loader = WorkflowInstanceLoader()

        self._data = None


    def setup(self, regions: np.ndarray, workflow_ID: str) -> (bool, [str]):
        # Utilize the Loaders to load the data from the database

        # Setup and load data from database
        results = {
            "_workflow_instance_to_instance_loader": [],
            "_workflow_instance_loader": [],
            "_datacenter_region_to_region_loader": [],
            "_datacenter_region_loader": [],
            "_carbon_region_to_region_loader": [],
            "_carbon_region_loader": []
        }
        
        results["_workflow_instance_to_instance_loader"].append(self._workflow_instance_to_instance_loader.setup(workflow_ID))
        results["_workflow_instance_loader"].append(self._workflow_instance_loader.setup(workflow_ID))
        
        results["_datacenter_region_to_region_loader"].append(self._datacenter_region_to_region_loader.setup(regions))
        results["_datacenter_region_loader"].append(self._datacenter_region_loader.setup(regions))
        results["_carbon_region_to_region_loader"].append(self._carbon_region_to_region_loader.setup(regions))
        results["_carbon_region_loader"].append(self._carbon_region_loader.setup(regions))
        
        all_success = all(result[0] for result_list in results.values() for result in result_list)
        reasons = [reason for result_list in results.values() for result in result_list if not result[0] for reason in result[1]]
        
        # Save the data into a dictionary for data sources
        self._data = {
            **self._workflow_instance_loader.retrieve_data(),
            **self._workflow_instance_to_instance_loader.retrieve_data(),
            **self._datacenter_region_loader.retrieve_data(),
            **self._datacenter_region_to_region_loader.retrieve_data(),
            **self._carbon_region_loader.retrieve_data(),
            **self._carbon_region_to_region_loader.retrieve_data()
        }

        if all_success:
            return True, ["Success"]
        else:
            return False, reasons

    def retrieve_data(self) -> dict:
        return self._data