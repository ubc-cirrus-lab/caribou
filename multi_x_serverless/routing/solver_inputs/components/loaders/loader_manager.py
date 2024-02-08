from typing import Any

from multi_x_serverless.routing.solver_inputs.components.loaders.carbon.carbon_region_from_to_loader import (
    CarbonRegionFromToLoader,
)
from multi_x_serverless.routing.solver_inputs.components.loaders.carbon.carbon_region_loader import CarbonRegionLoader
from multi_x_serverless.routing.solver_inputs.components.loaders.datacenter.datacenter_region_loader import (
    DatacenterRegionLoader,
)
from multi_x_serverless.routing.solver_inputs.components.loaders.datacenter.datacenter_region_to_region_loader import (
    DatacenterRegionToRegionLoader,
)
from multi_x_serverless.routing.solver_inputs.components.loaders.workflow.workflow_instance_from_to_loader import (
    WorkflowInstanceFromToLoader,
)
from multi_x_serverless.routing.solver_inputs.components.loaders.workflow.workflow_instance_loader import (
    WorkflowInstanceLoader,
)


class LoaderManager:
    def __init__(self) -> None:
        # initialize components
        self._datacenter_region_to_region_loader = DatacenterRegionToRegionLoader()
        self._datacenter_region_loader = DatacenterRegionLoader()
        self._carbon_region_to_region_loader = CarbonRegionFromToLoader()
        self._carbon_region_loader = CarbonRegionLoader()
        self._workflow_instance_to_instance_loader = WorkflowInstanceFromToLoader()
        self._workflow_instance_loader = WorkflowInstanceLoader()
        self._data: dict[str, Any] = {}

    def setup(self, regions: list[tuple[str, str]], workflow_id: str) -> bool:
        # Utilize the Loaders to load the data from the database

        # Setup and load data from database
        results = {
            "_workflow_instance_to_instance_loader": self._workflow_instance_to_instance_loader.setup(workflow_id),
            "_workflow_instance_loader": self._workflow_instance_loader.setup(workflow_id),
            "_datacenter_region_to_region_loader": self._datacenter_region_to_region_loader.setup(regions),
            "_datacenter_region_loader": self._datacenter_region_loader.setup(regions),
            "_carbon_region_to_region_loader": self._carbon_region_to_region_loader.setup(regions),
            "_carbon_region_loader": self._carbon_region_loader.setup(regions),
        }

        all_success = all(value for value in results.values())

        # Save the data into a dictionary for data sources
        self._data.update(
            {
                **self._workflow_instance_loader.retrieve_data(),
                **self._workflow_instance_to_instance_loader.retrieve_data(),
                **self._datacenter_region_loader.retrieve_data(),
                **self._datacenter_region_to_region_loader.retrieve_data(),
                **self._carbon_region_loader.retrieve_data(),
                **self._carbon_region_to_region_loader.retrieve_data(),
            }
        )

        return all_success

    def retrieve_data(self) -> dict:
        return self._data
