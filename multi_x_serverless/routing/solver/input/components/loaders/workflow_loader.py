from typing import Any

from multi_x_serverless.common.constants import WORKFLOW_INSTANCE_TABLE
from multi_x_serverless.deployment.common.remote_client.remote_client import RemoteClient
from multi_x_serverless.routing.solver.input.components.loader import InputLoader


class WorkflowLoader(InputLoader):
    _workflow_data: dict[str, Any]

    def __init__(self, client: RemoteClient) -> None:
        super().__init__(client, WORKFLOW_INSTANCE_TABLE)

    def setup(self, workflow_id: str) -> None:
        self._workflow_data = self._retrieve_workflow_data(workflow_id)

    def get_workflow_data(self) -> dict[str, Any]:
        return self._workflow_data

    def _retrieve_workflow_data(self, workflow_id: str) -> dict[str, Any]:
        return self._retrive_data(self._primary_table, workflow_id)
