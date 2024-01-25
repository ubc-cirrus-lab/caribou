from typing import Any

from multi_x_serverless.data_collectors.data_collector import DataCollector

class CarbonCollector(DataCollector):
    def __init__(self) -> None:
        super().__init__()
        self._table_key = "carbon_data"
        

    # def collect_data(self) -> dict[str, Any]:
    #     # TODO (#50): Fill Data Collector Implementations

    #     # See multi_x_serverless/routing/solver_inputs/components/loaders/carbon
    #     # for how to load data to the database to be retrieved there
    #     return {}

# class InstanceToInstanceWorkflowDataCollector(DataCollector):
#     def __init__(self) -> None:
#         self._table_key = "instance_to_instance_workflow_data"
#         super().__init__()

#     def collect_data(self) -> dict[str, Any]:
#         # TODO (#50): Fill Data Collector Implementations

#         # See multi_x_serverless/routing/solver_inputs/components/loaders/workflow
#         # for how to load data to the database to be retrieved there
#         return {}
