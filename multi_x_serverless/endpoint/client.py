import json
import logging
from typing import Any, Optional
import random

import botocore.exceptions

from multi_x_serverless.common.constants import (
    DEPLOYMENT_MANAGER_RESOURCE_TABLE,
    MULTI_X_SERVERLESS_WORKFLOW_IMAGES_TABLE,
    SOLVER_UPDATE_CHECKER_RESOURCE_TABLE,
    WORKFLOW_INSTANCE_TABLE,
    WORKFLOW_PLACEMENT_DECISION_TABLE,
    WORKFLOW_PLACEMENT_SOLVER_STAGING_AREA_TABLE,
)
from multi_x_serverless.common.models.endpoints import Endpoints
from multi_x_serverless.common.models.remote_client.aws_remote_client import AWSRemoteClient
from multi_x_serverless.common.models.remote_client.remote_client_factory import RemoteClientFactory
from multi_x_serverless.routing.solver.bfs_fine_grained_solver import BFSFineGrainedSolver
from multi_x_serverless.routing.solver.coarse_grained_solver import CoarseGrainedSolver
from multi_x_serverless.routing.solver.solver import Solver
from multi_x_serverless.routing.solver.stochastic_heuristic_descent_solver import StochasticHeuristicDescentSolver
from multi_x_serverless.routing.workflow_config import WorkflowConfig

logger = logging.getLogger(__name__)


class Client:
    def __init__(self, workflow_id: Optional[str] = None) -> None:
        self._workflow_id = workflow_id
        self._endpoints = Endpoints()
        self._home_region_threshold = 0.1  # 10% of the time run in home region

    def run(self, input_data: Optional[str] = None) -> None:
        if self._workflow_id is None:
            raise RuntimeError("No workflow id provided")

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

        send_to_home_region = random.random() < self._home_region_threshold

        provider, region, identifier = self.__get_initial_node_workflow_placement_decision(
            workflow_placement_decision, send_to_home_region
        )

        wrapped_input_data = {"input_data": input_data, "send_to_home_region": send_to_home_region}

        json_payload = json.dumps(wrapped_input_data)

        RemoteClientFactory.get_remote_client(provider, region).invoke_function(
            message=json_payload,
            identifier=identifier,
            workflow_instance_id="0",
        )

    def __get_initial_node_workflow_placement_decision(
        self, workflow_placement_decision: dict[str, Any], send_to_home_region: bool
    ) -> tuple[str, str, str]:
        initial_instance_name = workflow_placement_decision["current_instance_name"]
        if send_to_home_region:
            key = "home_deployment"
        else:
            key = "current_deployment"
        provider_region = workflow_placement_decision["workflow_placement"][key]["instances"][initial_instance_name][
            "provider_region"
        ]
        identifier = workflow_placement_decision["workflow_placement"][key]["instances"][initial_instance_name][
            "identifier"
        ]
        return provider_region["provider"], provider_region["region"], identifier

    def list_workflows(self) -> None:
        deployed_workflows = self._endpoints.get_solver_update_checker_client().get_keys(
            SOLVER_UPDATE_CHECKER_RESOURCE_TABLE
        )

        if deployed_workflows is None:
            print("No workflows deployed")
            return
        print("Deployed workflows:")
        for workflow in deployed_workflows:
            print(workflow)

    def remove(self) -> None:
        if self._workflow_id is None:
            raise RuntimeError("No workflow id provided")

        self._endpoints.get_solver_workflow_placement_decision_client().remove_key(
            WORKFLOW_PLACEMENT_DECISION_TABLE, self._workflow_id
        )
        self._endpoints.get_solver_update_checker_client().remove_key(
            WORKFLOW_PLACEMENT_SOLVER_STAGING_AREA_TABLE, self._workflow_id
        )
        self._endpoints.get_solver_update_checker_client().remove_key(
            SOLVER_UPDATE_CHECKER_RESOURCE_TABLE, self._workflow_id
        )

        currently_deployed_workflows = self._endpoints.get_deployment_manager_client().get_all_values_from_table(
            DEPLOYMENT_MANAGER_RESOURCE_TABLE
        )

        for workflow_id, deployment_manager_config_json in currently_deployed_workflows.items():
            if workflow_id != self._workflow_id:
                continue
            if not isinstance(deployment_manager_config_json, str):
                raise RuntimeError(
                    f"The deployment manager resource value for workflow_id: {workflow_id} is not a string"
                )
            self._remove_workflow(deployment_manager_config_json)

        self._endpoints.get_deployment_manager_client().remove_resource(f"deployment_package_{self._workflow_id}")

        self._endpoints.get_deployment_manager_client().remove_key(DEPLOYMENT_MANAGER_RESOURCE_TABLE, self._workflow_id)

        self._endpoints.get_deployment_manager_client().remove_key(
            MULTI_X_SERVERLESS_WORKFLOW_IMAGES_TABLE, self._workflow_id
        )

        self._endpoints.get_data_collector_client().remove_key(
            WORKFLOW_INSTANCE_TABLE, self._workflow_id.replace(".", "_")
        )

        print(f"Removed workflow {self._workflow_id}")

    def _remove_workflow(self, deployment_manager_config_json: str) -> None:
        deployment_manager_config = json.loads(deployment_manager_config_json)
        deployed_region_json = deployment_manager_config.get("deployed_regions")
        deployed_region: dict[str, dict[str, Any]] = json.loads(deployed_region_json)

        for function_physical_instance, provider_region in deployed_region.items():
            self._remove_function_instance(function_physical_instance, provider_region["deploy_region"])

    def _remove_function_instance(self, function_instance: str, provider_region: dict[str, str]) -> None:
        provider = provider_region["provider"]
        region = provider_region["region"]
        identifier = function_instance
        role_name = f"{identifier}-role"
        messaging_topic_name = f"{identifier}_messaging_topic"
        client = RemoteClientFactory.get_remote_client(provider, region)

        try:
            if isinstance(client, AWSRemoteClient):
                client.remove_ecr_repository(identifier)
        except RuntimeError as e:
            print(f"Could not remove ecr repository {identifier}: {str(e)}")
        except botocore.exceptions.ClientError as e:
            print(f"Could not remove ecr repository {identifier}: {str(e)}")

        try:
            topic_identifier = client.get_topic_identifier(messaging_topic_name)
            client.remove_messaging_topic(topic_identifier)
        except RuntimeError as e:
            print(f"Could not remove messaging topic {messaging_topic_name}: {str(e)}")

        try:
            client.remove_function(identifier)
        except RuntimeError as e:
            print(f"Could not remove function {identifier}: {str(e)}")
        except botocore.exceptions.ClientError as e:
            print(f"Could not remove role {role_name}: {str(e)}")

        print(f"Removed function {function_instance} from provider {provider} in region {region}")

    def solve(self, solver: Optional[str] = None) -> None:
        if self._workflow_id is None:
            raise RuntimeError("No workflow id provided")

        workflow_information = self._endpoints.get_solver_update_checker_client().get_value_from_table(
            SOLVER_UPDATE_CHECKER_RESOURCE_TABLE, self._workflow_id
        )

        if workflow_information is None:
            raise RuntimeError(f"No workflow with id {self._workflow_id} found")

        workflow_information_dict = json.loads(workflow_information)

        if "workflow_config" not in workflow_information_dict:
            raise RuntimeError(f"Workflow with id {self._workflow_id} has no workflow_config")

        workflow_config_json = workflow_information_dict["workflow_config"]

        workflow_config = json.loads(workflow_config_json)

        workflow_config_instance = WorkflowConfig(workflow_config)

        solver_instance: Optional[Solver] = None

        if solver is None or solver == "coarse-grained":
            solver_instance = CoarseGrainedSolver(workflow_config_instance)
        elif solver == "fine-grained":
            solver_instance = BFSFineGrainedSolver(workflow_config_instance)
        elif solver == "heuristic":
            solver_instance = StochasticHeuristicDescentSolver(workflow_config_instance)
        else:
            raise ValueError(f"Solver {solver} not supported")

        if solver_instance is None:
            raise RuntimeError("Solver instance is None")

        solver_instance.solve()
