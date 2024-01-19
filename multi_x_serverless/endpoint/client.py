import json
from typing import Any

from multi_x_serverless.common.constants import WORKFLOW_PLACEMENT_DECISION_TABLE
from multi_x_serverless.common.models.endpoints import Endpoints
from multi_x_serverless.deployment.common.factories.remote_client_factory import RemoteClientFactory


class Client:
    def __init__(self, workflow_name: str) -> None:
        self._workflow_name = workflow_name
        self._endpoints = Endpoints()

    def run(self, input_data: dict) -> None:
        result = self._endpoints.get_solver_workflow_placement_decision_client().get_value_from_table(
            WORKFLOW_PLACEMENT_DECISION_TABLE, self._workflow_name
        )

        if result is None:
            raise RuntimeError("No workflow placement decision found for workflow")

        workflow_placement_decision = json.loads(result)

        provider, region, identifier = self.__get_initial_node_workflow_placement_decision(workflow_placement_decision)

        json_payload = json.dumps(input_data)

        RemoteClientFactory.get_remote_client(provider, region).invoke_function(
            message=json_payload,
            identifier=identifier,
            workflow_instance_id="0",
            merge=False,
            function_name=None,
            expected_counter=-1,
        )

    def __get_initial_node_workflow_placement_decision(self, workflow_placement_decision: dict[str, Any]) -> tuple[str, str, str]:
        initial_instance_name = workflow_placement_decision["current_instance_name"]
        provider_region = workflow_placement_decision["workflow_placement"][initial_instance_name]["provider_region"]
        identifier = workflow_placement_decision["workflow_placement"][initial_instance_name]["identifier"]
        return provider_region["provider"], provider_region["region"], identifier
