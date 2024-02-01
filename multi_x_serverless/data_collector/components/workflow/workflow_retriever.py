from typing import Any

from multi_x_serverless.common.constants import WORKFLOW_PLACEMENT_DECISION_TABLE, WORKFLOW_SUMMARY_TABLE
from multi_x_serverless.data_collector.components.data_retriever import DataRetriever
from multi_x_serverless.deployment.common.remote_client.remote_client import RemoteClient


class WorkflowRetriever(DataRetriever):
    def __init__(self, client: RemoteClient) -> None:
        super().__init__(client)
        self._available_workflow_table: str = WORKFLOW_PLACEMENT_DECISION_TABLE
        self._workflow_summary_table: str = WORKFLOW_SUMMARY_TABLE

    def retrieve_all_workflow_ids(self) -> list[str]:
        # Perhaps there could be a get all keys method in the remote client
        return list(self._client.get_all_values_from_table(self._workflow_summary_table).keys())

    def retrieve_workflow_summary(self, workflow_unique_id: str) -> dict[str, Any]:
        # # No associated legacy code
        # - Key: `<workflow_unique_id>`
        # - Sort Key (N): Timestamp of last summary (last summarized by Datastore Syncer)
        # - Value (S):
        #   - Number of total invocations (For the entire workflow)
        #   - Time between last summary to current summary
        #   - At Instance `<instance_unique_id>`
        #     Number of total invocation of this instance
        #     - At Region `<provider_unique_id>:<region_name>`
        #       - Number of invocation (of this instance in this region)
        #       - Region Average/Tail Runtime.
        #     - To Instance `<instance_unique_id>`
        #       - Number of calls from parent instance to this instance.
        #       - Average data transfer size between instance stages.
        #       - At Region `<provider_unique_id>:<region_name>`
        #         - To Region `<provider_unique_id>:<region_name>`
        #           - Number transmission
        #           - Region Average/Tail Latency.

        # TODO -> Parse the workflow summary to get the workflow summary
        self._client.get_value_from_table(self._workflow_summary_table, workflow_unique_id)
        return {}
