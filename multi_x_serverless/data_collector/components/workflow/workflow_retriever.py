from typing import Any

from multi_x_serverless.common.constants import WORKFLOW_PLACEMENT_DECISION_TABLE
from multi_x_serverless.data_collector.components.data_retriever import DataRetriever
from multi_x_serverless.deployment.common.remote_client.remote_client import RemoteClient


class WorkflowRetriever(DataRetriever):
    def __init__(self, client: RemoteClient) -> None:
        super().__init__(client)
        self._available_workflow_table = WORKFLOW_PLACEMENT_DECISION_TABLE

    def retrieve_available_workflows(self) -> dict[str, dict[str, Any]]:
        return self._client.get_all_values_from_table(self._available_workflow_table)


# No associated legacy code
