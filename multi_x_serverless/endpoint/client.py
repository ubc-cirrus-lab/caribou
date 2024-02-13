import json
from typing import Any, Optional

from multi_x_serverless.common.constants import WORKFLOW_PLACEMENT_DECISION_TABLE
from multi_x_serverless.common.models.endpoints import Endpoints
from multi_x_serverless.common.models.remote_client.remote_client_factory import RemoteClientFactory


class Client:
    def __init__(self, workflow_id: str) -> None:
        self._workflow_id = workflow_id
        self._endpoints = Endpoints()

    def run(self, input_data: Optional[str] = None) -> None:
        result = self._endpoints.get_solver_workflow_placement_decision_client().get_value_from_table(
            WORKFLOW_PLACEMENT_DECISION_TABLE, self._workflow_id
        )

        if input_data is None:
            input_data = ""

        if result is None or result == "":
            raise RuntimeError(
                f"No workflow placement decision found for workflow, did you deploy the workflow and is the workflow id ({self._workflow_id}) correct?"  # pylint: disable=line-too-long
            )

        workflow_placement_decision = json.loads(result)

        provider, region, identifier = self.__get_initial_node_workflow_placement_decision(workflow_placement_decision)

        json_payload = json.dumps(input_data)

        RemoteClientFactory.get_remote_client(provider, region).invoke_function(
            message=json_payload,
            identifier=identifier,
            workflow_instance_id="0",
        )

    def __get_initial_node_workflow_placement_decision(
        self, workflow_placement_decision: dict[str, Any]
    ) -> tuple[str, str, str]:
        initial_instance_name = workflow_placement_decision["current_instance_name"]
        provider_region = workflow_placement_decision["workflow_placement"][initial_instance_name]["provider_region"]
        identifier = workflow_placement_decision["workflow_placement"][initial_instance_name]["identifier"]
        return provider_region["provider"], provider_region["region"], identifier
