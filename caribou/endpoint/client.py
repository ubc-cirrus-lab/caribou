import json
import logging
import random
import uuid
from datetime import datetime
from typing import Any, Optional

import botocore.exceptions

from caribou.common.constants import (
    CARIBOU_WORKFLOW_IMAGES_TABLE,
    DEPLOYMENT_MANAGER_RESOURCE_TABLE,
    DEPLOYMENT_RESOURCES_TABLE,
    GLOBAL_TIME_ZONE,
    HOME_REGION_THRESHOLD,
    TIME_FORMAT,
    WORKFLOW_INSTANCE_TABLE,
    WORKFLOW_PLACEMENT_DECISION_TABLE,
    WORKFLOW_PLACEMENT_SOLVER_STAGING_AREA_TABLE,
    WORKFLOW_SUMMARY_TABLE,
)
from caribou.common.models.endpoints import Endpoints
from caribou.common.models.remote_client.aws_remote_client import AWSRemoteClient
from caribou.common.models.remote_client.remote_client import RemoteClient
from caribou.common.models.remote_client.remote_client_factory import RemoteClientFactory

# Set logging level for Boto3 to WARNING to suppress INFO messages
# Mainly to suppress 'Found credentials in environment variables.' message
# May in the future want to make it just targetting the specific logger
logging.getLogger("botocore").setLevel(logging.WARNING)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())


class Client:
    def __init__(self, workflow_id: Optional[str] = None) -> None:
        self._workflow_id = workflow_id
        self._endpoints = Endpoints()
        self._home_region_threshold = HOME_REGION_THRESHOLD  # fractional % of the time run in home region

        # Cache the remote clients (one per provider-region pair)
        self._remote_clients: dict[str, RemoteClient] = {}

    def run(self, input_data: Optional[str] = None) -> str:
        current_time = datetime.now(GLOBAL_TIME_ZONE).strftime(TIME_FORMAT)

        if self._workflow_id is None:
            raise RuntimeError("No workflow id provided")

        (
            raw_wpd,
            consumed_read_capacity,
        ) = self._endpoints.get_deployment_algorithm_workflow_placement_decision_client().get_value_from_table(
            WORKFLOW_PLACEMENT_DECISION_TABLE, self._workflow_id
        )

        if input_data is None:
            input_data = ""

        if raw_wpd is None or raw_wpd == "":
            raise RuntimeError(
                f"No workflow placement decision found for workflow, did you deploy the workflow and is the workflow id ({self._workflow_id}) correct?"  # pylint: disable=line-too-long
            )

        # Get the WPD size
        wpd_data_size = len(raw_wpd.encode("utf-8")) / (1024**3)
        workflow_placement_decision = json.loads(raw_wpd)

        send_to_home_region = random.random() < self._home_region_threshold

        workflow_placement_decision["time_key"] = self._get_time_key(workflow_placement_decision)

        print(f"Sending to home region: {send_to_home_region}, time_key: {workflow_placement_decision['time_key']}")

        provider, region, identifier = self._get_initial_node_workflow_placement_decision(
            workflow_placement_decision, send_to_home_region
        )

        workflow_placement_decision["send_to_home_region"] = send_to_home_region

        run_id = uuid.uuid4().hex
        workflow_placement_decision["run_id"] = run_id  # Run_id is stored in the workflow_placement_decision
        workflow_placement_decision["data_size"] = wpd_data_size
        workflow_placement_decision["consumed_read_capacity"] = consumed_read_capacity
        print(f"Run ID for current run: {run_id}")
        wrapped_input_data = {
            "payload": input_data,
            "time_request_sent": current_time,
            "workflow_placement_decision": workflow_placement_decision,
            "number_of_hops_from_client_request": 0,
            "permit_redirection": False,  # We don't want to redirect the request.
            "redirected": False,
            "request_source": "Caribou CLI",
        }

        json_payload = json.dumps(wrapped_input_data)
        self._get_remote_client(provider, region).invoke_function(
            message=json_payload,
            identifier=identifier,
        )

        return run_id

    def _get_remote_client(self, provider: str, region: str) -> RemoteClient:
        # Check if it is already in the cache
        key = f"{provider}-{region}"
        if key in self._remote_clients:
            return self._remote_clients[key]

        # Create a new remote client
        remote_client = RemoteClientFactory.get_remote_client(provider, region)
        self._remote_clients[key] = remote_client

        return remote_client

    def _get_time_key(self, workflow_placement_decision: dict[str, Any]) -> str:
        if "current_deployment" not in workflow_placement_decision["workflow_placement"]:
            return "N/A"

        all_time_keys = workflow_placement_decision["workflow_placement"]["current_deployment"]["time_keys"]

        current_hour_of_day = datetime.now(GLOBAL_TIME_ZONE).hour

        previous_time_key = max(time_key for time_key in all_time_keys if int(time_key) <= current_hour_of_day)
        return previous_time_key

    def _get_initial_node_workflow_placement_decision(
        self, workflow_placement_decision: dict[str, Any], send_to_home_region: bool
    ) -> tuple[str, str, str]:
        initial_instance_name = workflow_placement_decision["current_instance_name"]
        key = self._get_deployment_key(workflow_placement_decision, send_to_home_region)
        if key == "current_deployment":
            provider_region = workflow_placement_decision["workflow_placement"]["current_deployment"]["instances"][
                workflow_placement_decision["time_key"]
            ][initial_instance_name]["provider_region"]
            identifier = workflow_placement_decision["workflow_placement"]["current_deployment"]["instances"][
                workflow_placement_decision["time_key"]
            ][initial_instance_name]["identifier"]
        else:
            provider_region = workflow_placement_decision["workflow_placement"]["home_deployment"][
                initial_instance_name
            ]["provider_region"]
            identifier = workflow_placement_decision["workflow_placement"]["home_deployment"][initial_instance_name][
                "identifier"
            ]
        return provider_region["provider"], provider_region["region"], identifier

    def _get_deployment_key(self, workflow_placement_decision: dict[str, Any], send_to_home_region: bool) -> str:
        key = "home_deployment"
        if send_to_home_region:
            return key

        if "current_deployment" not in workflow_placement_decision["workflow_placement"]:
            return key

        # Check if the deployment is not expired
        deployment_expiry_time = workflow_placement_decision["workflow_placement"]["current_deployment"].get(
            "expiry_time", None
        )
        if deployment_expiry_time is not None:
            # If the deployment is expired, return the home deployment
            if datetime.now(GLOBAL_TIME_ZONE) <= datetime.strptime(deployment_expiry_time, TIME_FORMAT):
                key = "current_deployment"

        return key

    def list_workflows(self) -> None:
        deployed_workflows = self._endpoints.get_deployment_manager_client().get_keys(DEPLOYMENT_MANAGER_RESOURCE_TABLE)

        if deployed_workflows is None:
            print("No workflows deployed")
            return
        print("Deployed workflows:")

        # Sort the workflows by name
        for workflow in sorted(deployed_workflows):
            print(workflow)

    def remove(self) -> None:
        if self._workflow_id is None:
            raise RuntimeError("No workflow id provided")

        self._endpoints.get_deployment_algorithm_workflow_placement_decision_client().remove_key(
            WORKFLOW_PLACEMENT_DECISION_TABLE, self._workflow_id
        )
        self._endpoints.get_deployment_manager_client().remove_key(
            WORKFLOW_PLACEMENT_SOLVER_STAGING_AREA_TABLE, self._workflow_id
        )
        self._endpoints.get_deployment_manager_client().remove_key(DEPLOYMENT_MANAGER_RESOURCE_TABLE, self._workflow_id)

        currently_deployed_workflows = self._endpoints.get_deployment_resources_client().get_all_values_from_table(
            DEPLOYMENT_RESOURCES_TABLE
        )

        for workflow_id, deployment_manager_config_json in currently_deployed_workflows.items():
            if workflow_id != self._workflow_id:
                continue
            if not isinstance(deployment_manager_config_json, str):
                raise RuntimeError(
                    f"The deployment manager resource value for workflow_id: {workflow_id} is not a string"
                )
            self._remove_workflow(deployment_manager_config_json)

        self._endpoints.get_deployment_resources_client().remove_resource(f"deployment_package_{self._workflow_id}")

        self._endpoints.get_deployment_resources_client().remove_key(DEPLOYMENT_RESOURCES_TABLE, self._workflow_id)

        self._endpoints.get_data_collector_client().remove_key(WORKFLOW_INSTANCE_TABLE, self._workflow_id)

        self._endpoints.get_deployment_resources_client().remove_key(
            CARIBOU_WORKFLOW_IMAGES_TABLE, self._workflow_id.replace(".", "_")
        )

        self._endpoints.get_datastore_client().remove_key(WORKFLOW_SUMMARY_TABLE, self._workflow_id)

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
