from multi_x_serverless.deployment.common.deploy.models.endpoints import Endpoints
from multi_x_serverless.deployment.common.constants import ROUTING_DECISION_TABLE
from multi_x_serverless.deployment.common.factories.remote_client_factory import RemoteClientFactory

from typing import Any

import json


class Client:
    def __init__(self, workflow_name: str) -> None:
        self._workflow_name = workflow_name
        self._endpoints = Endpoints()

    def run(self, input_data: dict) -> None:
        result = self._endpoints.get_solver_routing_decision_client().get_value_from_table(
            ROUTING_DECISION_TABLE, self._workflow_name
        )

        if result is None:
            raise RuntimeError("No routing decision found for workflow")

        routing_decision = json.loads(result)

        provider, region, identifier = self.__get_initial_node_routing_decision(routing_decision)

        json_payload = json.dumps(input_data)

        RemoteClientFactory.get_remote_client(provider, region).invoke_function(
            message=json_payload,
            identifier=identifier,
            workflow_instance_id="0",
            merge=False,
            function_name=None,
            expected_counter=-1,
        )

    def __get_initial_node_routing_decision(self, routing_decision: dict[str, Any]) -> tuple[str, str]:
        initial_instance_name = routing_decision["current_instance_name"]
        provider_region = routing_decision["routing_placement"][initial_instance_name]["provider_region"]
        provider = provider_region["provider"]
        region = provider_region["region"]
        identifier = routing_decision["routing_placement"][initial_instance_name]["identifier"]
        return provider, region, identifier
