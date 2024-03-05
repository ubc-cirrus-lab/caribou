from typing import Dict, List, Optional

from pydantic import BaseModel, Field

from multi_x_serverless.deployment.common.config.config_schema import Constraints, ProviderRegion, RegionAndProviders


class Instance(BaseModel):
    instance_name: str = Field(..., title="The name of the instance")
    regions_and_providers: RegionAndProviders = Field(..., title="List of regions and providers")
    succeeding_instances: List[str] = Field(..., title="List of succeeding instances")
    preceding_instances: List[str] = Field(..., title="List of preceding instances")


class WorkflowConfigSchema(BaseModel):
    workflow_name: str = Field(..., title="The name of the workflow")
    workflow_version: str = Field(..., title="The version of the workflow")
    workflow_id: str = Field(..., title="The id of the workflow")
    regions_and_providers: RegionAndProviders = Field(..., title="List of regions and providers")
    instances: Dict[str, Instance] = Field(..., title="List of instances")
    constraints: Constraints = Field(..., title="Constraints")
    start_hops: List[ProviderRegion] = Field(..., title="List of home regions")
    num_calls_in_one_month: Optional[int] = Field(None, title="Number of function calls in 1 hour")
    solver: Optional[str] = Field(None, title="The name of the solver")
