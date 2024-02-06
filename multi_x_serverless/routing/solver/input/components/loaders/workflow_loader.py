from typing import Any

from typing import Optional

from multi_x_serverless.common.constants import WORKFLOW_INSTANCE_TABLE
from multi_x_serverless.common.models.remote_client.remote_client import RemoteClient
from multi_x_serverless.routing.solver.input.components.loader import InputLoader


class WorkflowLoader(InputLoader):
    _workflow_data: dict[str, Any]

    def __init__(self, client: RemoteClient) -> None:
        super().__init__(client, WORKFLOW_INSTANCE_TABLE)

    def setup(self, workflow_id: str) -> None:
        self._workflow_data = self._retrieve_workflow_data(workflow_id)

    def get_runtime(self, instance_name: str, region_name: str, use_tail_runtime: bool = False) -> float:
        runtime_type = "tail_runtime" if use_tail_runtime else "average_runtime"
        return self._workflow_data.get(instance_name, {}).get("execution_summary", {}).get(region_name, {}).get(runtime_type, -1)
    
    def get_latency(self, from_instance_name: str, to_instance_name: str, from_region_name: str, to_region_name: str, use_tail_runtime: bool = False) -> float:
        latency_type = "tail_latency" if use_tail_runtime else "average_latency"
        return self._workflow_data.get(from_instance_name, {}).get("invocation_summary", {}).get(to_instance_name, {}).get("transmission_summary", {}).get(from_region_name, {}).get(to_region_name, {}).get(latency_type, -1)

    def get_data_transfer_size(self, from_instance_name: str, to_instance_name: str) -> float:
        return self._workflow_data.get(from_instance_name, {}).get("invocation_summary", {}).get(to_instance_name, {}).get("average_data_transfer_size", 0)

    def get_invocation_probability(self, from_instance_name: str, to_instance_name: str) -> float:
        if from_instance_name == to_instance_name: # Special case for start node
            return 1
        
        return self._workflow_data.get(from_instance_name, {}).get("invocation_summary", {}).get(to_instance_name, {}).get("probability_of_invocation", 0) # Possible to have 0 if never called

    def get_favourite_region(self, instance_name: str) -> Optional[str]:
        return self._workflow_data.get(instance_name, {}).get("favourite_home_region", None)
    
    def get_favourite_region_runtime(self, instance_name: str, use_tail_runtime: bool = False) -> float:
        # This instance MUST exist in the workflow data for this to ever be called
        if use_tail_runtime:
            return self._workflow_data.get(instance_name).get("favourite_home_region_tail_runtime")
        else:
            return self._workflow_data.get(instance_name).get("favourite_home_region_average_runtime")

    def get_projected_monthly_invocations(self, instance_name: str) -> float:
        return self._workflow_data.get(instance_name, {}).get("projected_monthly_invocations", 0)
    
    def _retrieve_workflow_data(self, workflow_id: str) -> dict[str, Any]:
        return self._retrive_data(self._primary_table, workflow_id)
