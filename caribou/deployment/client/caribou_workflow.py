from __future__ import annotations

import ast
import base64
import binascii
import json
import logging
import uuid
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime
from typing import Any, Callable, Optional, cast

from caribou.common.constants import (
    GLOBAL_TIME_ZONE,
    LOG_VERSION,
    MAX_WORKERS,
    TIME_FORMAT,
    WORKFLOW_PLACEMENT_DECISION_TABLE,
    MAX_TRANSFER_SIZE,
)
from caribou.common.models.endpoints import Endpoints
from caribou.common.models.remote_client.remote_client_factory import RemoteClientFactory
from caribou.common.utils import get_function_source
from caribou.deployment.client.caribou_function import CaribouFunction

# Alter the logging to use CARIBOU level instead of info
## Define a custom logging level
CARIBOU_LEVEL = 25
logging.addLevelName(CARIBOU_LEVEL, "CARIBOU")


## Custom logger class
class CustomLogger(logging.Logger):
    def caribou(self, message: str, *args: Any, **kws: Any) -> None:
        if self.isEnabledFor(CARIBOU_LEVEL):
            self._log(CARIBOU_LEVEL, message, args, **kws)


## Replace the default logger class with your custom logger class
logging.setLoggerClass(CustomLogger)

## Create an instance of your custom logger class
logger = cast(CustomLogger, logging.getLogger(__name__))

## Set the logging level
logger.setLevel(logging.INFO)


class CustomEncoder(json.JSONEncoder):
    def default(self, o: Any) -> Any:
        if isinstance(o, bytes):
            return "b64:" + base64.b64encode(o).decode()
        return json.JSONEncoder.default(self, o)


class CustomDecoder(json.JSONDecoder):
    def decode(self, s, _w=json.decoder.WHITESPACE.match):  # type: ignore
        decoded_dict = super().decode(s)
        self.decode_values(decoded_dict)
        return decoded_dict

    def decode_values(self, item: Any) -> None:
        if isinstance(item, dict):
            for key, value in item.items():
                if isinstance(value, str) and value.startswith("b64:"):
                    try:
                        # Remove the prefix before decoding
                        item[key] = base64.b64decode(value[4:])
                    except (TypeError, binascii.Error):
                        pass
                else:
                    self.decode_values(value)
        elif isinstance(item, list):
            for i, value in enumerate(item):
                if isinstance(value, str) and value.startswith("b64:"):
                    try:
                        # Remove the prefix before decoding
                        item[i] = base64.b64decode(value[4:])
                    except (TypeError, binascii.Error):
                        pass
                else:
                    self.decode_values(value)


class CaribouWorkflow:  # pylint: disable=too-many-instance-attributes
    """
    CaribouWorkflow class that is used to register functions as a collection of connected serverless functions.

    Every workflow must have an instance of this class.
    The instance is used to register functions as serverless functions.

    Every workflow must have one function that is the entry point for the workflow. This function must be registered
    with the `serverless_function` decorator with the `entry_point` parameter set to `True`.

    :param name: The name of the workflow.
    """

    def __init__(self, name: str, version: str):
        self.name = name
        self.version = version
        self.functions: dict[str, CaribouFunction] = {}
        self._run_id_to_successor_index: dict[str, int] = {}
        self._function_names: set[str] = set()
        self._endpoint = Endpoints()
        self._current_workflow_placement_decision: dict[str, Any] = {}

        # For thread pool -> Invoke successor functions asynchronously
        self._thread_pool: ThreadPoolExecutor = ThreadPoolExecutor(max_workers=MAX_WORKERS)
        self._futures: list[Future] = []

        # For logging
        ## This will be overritten by the first function that is called
        ## Just here as a placeholder to avoid using None
        self._function_start_time: datetime = datetime.now(GLOBAL_TIME_ZONE)

    def get_run_id(self) -> str:
        return self._current_workflow_placement_decision["run_id"]

    def get_successors(self, function: CaribouFunction) -> list[CaribouFunction]:
        """
        Get the functions that are called by this function.
        """
        source_code = get_function_source(function.function_callable)
        tree = ast.parse(source_code)
        function_calls = [
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.Call) and getattr(node.func, "attr", None) == "invoke_serverless_function"
        ]
        successors: list[CaribouFunction] = []
        for call in function_calls:
            function_name = call.args[0].id  # type: ignore
            successor = next(
                (func for func in self.functions.values() if func.function_callable.__name__ == function_name), None
            )
            if successor:
                successors.append(successor)
            else:
                raise RuntimeError(f"Could not find function with name {function_name}, was the function registered?")
        return successors

    def invoke_serverless_function(
        self,
        function: Callable[..., Any],
        payload: Optional[dict | Any] = None,
        conditional: bool = True,
    ) -> None:
        """
        Invoke a serverless function which is part of this workflow.
        """
        # If the function from which this function is called is the entry point obtain current
        # workflow_placement decision
        # If not, the workflow_placement decision was stored in the message received from the
        # predecessor function
        # Post message to SNS -> return
        # Do not wait for response
        workflow_placement_decision = self.get_workflow_placement_decision()

        current_instance_name = workflow_placement_decision["current_instance_name"]

        successor_instance_name, successor_workflow_placement_decision_dictionary = self.get_successor_instance_name(
            function, workflow_placement_decision
        )

        def invoke_worker(
            invocation_start_time: datetime,
            json_payload: str,
            alternative_json_payload: Optional[str],
            transmission_taint: str,
            conditional: bool,
        ) -> None:
            time_from_function_start = (invocation_start_time - self._function_start_time).total_seconds()

            provider, region, identifier = self.get_successor_workflow_placement_decision(
                successor_instance_name, workflow_placement_decision
            )

            if not conditional:
                # For the sync nodes we still need to inform the platform that the function has finished.
                (
                    total_sync_data_response_size,
                    total_consumed_capacity,
                    sync_nodes_invoked_logs,
                ) = self._inform_sync_node_of_conditional_non_execution(
                    workflow_placement_decision, successor_instance_name, current_instance_name
                )

                for sync_nodes_invoked_info in sync_nodes_invoked_logs:
                    finish_time: datetime = sync_nodes_invoked_info["finish_time"]
                    call_start_time: datetime = sync_nodes_invoked_info["call_start_time"]
                    log_message = (
                        f"INVOKING_SYNC_NODE: INSTANCE ({current_instance_name}) to "
                        f"SUCCESSOR ({successor_instance_name}) for "
                        f"PREDECESSOR_INSTANCE ({sync_nodes_invoked_info['predecessor']}) calling "
                        f"SYNC_NODE ({sync_nodes_invoked_info['sync_node']}) with "
                        f"PAYLOAD_SIZE ({sync_nodes_invoked_info['payload_size']}) GB and "
                        f"TAINT ({sync_nodes_invoked_info['transmission_taint']}) to "
                        f"PROVIDER ({sync_nodes_invoked_info['provider']}) and "
                        f"REGION ({sync_nodes_invoked_info['region']}) with "
                        f"INVOCATION_TIME_FROM_FUNCTION_START ({time_from_function_start}) s and "
                        f"FINISH_TIME_FROM_INVOCATION_START "
                        f"({(finish_time - invocation_start_time).total_seconds()}) s and "
                        f"CALL_START_TO_FINISH ({(finish_time - call_start_time).total_seconds()}) s"
                    )
                    self.log_for_retrieval(log_message, workflow_placement_decision["run_id"], invocation_start_time)

                # We don't call the function if it is conditional and the condition is not met.
                log_message = (
                    f"CONDITIONAL_NON_EXECUTION: INSTANCE ({current_instance_name}) calling "
                    f"SUCCESSOR ({successor_instance_name}) consuming "
                    f"CONSUMED_WRITE_CAPACITY ({total_consumed_capacity}) with "
                    f"SYNC_DATA_RESPONSE_SIZE ({total_sync_data_response_size}) GB to "
                    f"PROVIDER ({provider}) and REGION ({region}) with "
                    f"INVOCATION_TIME_FROM_FUNCTION_START ({time_from_function_start}) s and "
                    f"FINISH_TIME_FROM_INVOCATION_START "
                    f"({(datetime.now(GLOBAL_TIME_ZONE) - invocation_start_time).total_seconds()}) s"
                )
                self.log_for_retrieval(log_message, workflow_placement_decision["run_id"])
                return

            is_successor_sync_node = successor_instance_name.split(":", maxsplit=2)[1] == "sync"
            expected_counter = -1
            if is_successor_sync_node:
                expected_counter = len(
                    set(workflow_placement_decision["instances"][successor_instance_name]["preceding_instances"])
                )

            (
                upload_payload_size,
                sync_data_response_size,
                successor_invoked,
                upload_rtt,
                total_consumed_write_capacity,
            ) = RemoteClientFactory.get_remote_client(provider, region).invoke_function(
                message=json_payload,
                identifier=identifier,
                workflow_instance_id=workflow_placement_decision["run_id"],
                sync=is_successor_sync_node,
                function_name=successor_instance_name,
                expected_counter=expected_counter,
                current_instance_name=current_instance_name,
                alternative_message=alternative_json_payload,
            )

            send_json_payload = json_payload
            if upload_payload_size:
                # If sending to sync, system alters the direct
                # payload without user payload data
                if alternative_json_payload:
                    send_json_payload = alternative_json_payload

            log_message = (
                f"INVOKING_SUCCESSOR: INSTANCE ({current_instance_name}) potentially calling "
                f"SUCCESSOR ({successor_instance_name}) with PAYLOAD_SIZE "
                f"({len(send_json_payload.encode('utf-8')) / (1024**3)}) GB and TAINT ({transmission_taint}) to "
                f"PROVIDER ({provider}) and REGION ({region}) "
                f"SUCCESSOR_INVOKED ({successor_invoked}) at "
                f"INVOCATION_TIME_FROM_FUNCTION_START ({time_from_function_start}) s and "
                f"FINISH_TIME_FROM_INVOCATION_START "
                f"({(datetime.now(GLOBAL_TIME_ZONE) - invocation_start_time).total_seconds()}) s"
                f" UPLOADED_DATA_TO_SYNC_TABLE ({(upload_payload_size is not None)})"
            )
            if upload_payload_size:  # Add the upload DATA SIZE to the log message
                log_message += (
                    f" UPLOAD_DATA_SIZE ({upload_payload_size}) GB consuming "
                    f"CONSUMED_WRITE_CAPACITY ({total_consumed_write_capacity}) loaded "
                    f"SYNC_DATA_RESPONSE_SIZE ({sync_data_response_size}) GB with "
                    f"UPLOAD_RTT ({upload_rtt}) s"
                )
            self.log_for_retrieval(log_message, workflow_placement_decision["run_id"], invocation_start_time)

        # Wrap the payload and add the workflow_placement decision
        transmission_taint = uuid.uuid4().hex
        payload_wrapper: dict[str, Any] = {
            "workflow_placement_decision": successor_workflow_placement_decision_dictionary,
            "transmission_taint": transmission_taint,
        }
        alternative_json_payload: Optional[str] = None
        if payload:
            # Get an version of the json payload without the "payload"
            # key to be used in case of a sync node
            alternative_json_payload = json.dumps(payload_wrapper, cls=CustomEncoder)
            payload_wrapper["payload"] = payload
        json_payload = json.dumps(payload_wrapper, cls=CustomEncoder)

        # Check the payload_size_byte size, if its too large, we need throw an error
        # We need to ensure that the payload that we send to SNS is below
        # 262,144 bytes (256 KB),
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sns/client/publish.html
        # For safety, we will set the limit to 256,000 bytes (250 KB)
        payload_size_byte = len(json_payload.encode("utf-8"))
        if payload_size_byte > MAX_TRANSFER_SIZE:
            log_message = (
                f"DEBUG_MESSAGE: PAYLOAD_SIZE "
                f"({payload_size_byte / (1024**3)}) GB "
                f"PAYLOAD_SIZE_BYTE ({payload_size_byte}) Bytes"
                f"Exceeds the limit of {MAX_TRANSFER_SIZE} bytes"
            )
            self.log_for_retrieval(
                log_message,
                workflow_placement_decision["run_id"],
            )

            raise ValueError(
                f"Payload size is too large, please reduce the size of the payload. "
                f"Current payload size is {payload_size_byte} bytes, please limit to"
                f"under 250,000 bytes."
            )

        # Start the invocation timer AFTER the successor instance name has been determined
        # As they will also be in the critical path
        invocation_start_time = datetime.now(GLOBAL_TIME_ZONE)
        if self._thread_pool is not None and isinstance(self._thread_pool, ThreadPoolExecutor):
            future: Future = self._thread_pool.submit(
                invoke_worker,
                invocation_start_time,
                json_payload,
                alternative_json_payload,
                transmission_taint,
                conditional,
            )
            self._futures.append(future)
        else:
            # Run the worker in the main thread
            invoke_worker(
                invocation_start_time, json_payload, alternative_json_payload, transmission_taint, conditional
            )
            log_message = (
                f"INVOKED_SYNCHRONOUSLY: INSTANCE ({current_instance_name}) calling "
                f"SUCCESSOR ({successor_instance_name}) were not invoked asynchronously"
            )
            self.log_for_retrieval(
                log_message,
                workflow_placement_decision["run_id"],
            )

    def _inform_sync_node_of_conditional_non_execution(
        self, workflow_placement_decision: dict[str, Any], successor_instance_name: str, current_instance_name: str
    ) -> tuple[float, float, list[dict[str, Any]]]:
        # Record the total consumed write capacity
        total_consumed_capacity = 0.0
        total_sync_data_response_size = 0.0
        sync_nodes_invoked_logs: list[dict[str, Any]] = []

        # If the successor is a sync node, we need to inform the platform that the function has finished.
        if successor_instance_name.split(":", maxsplit=2)[1] == "sync":
            response_size, consumed_capacity = self._inform_and_invoke_sync_node(
                workflow_placement_decision, successor_instance_name, current_instance_name, sync_nodes_invoked_logs
            )

            total_sync_data_response_size += response_size
            total_consumed_capacity += consumed_capacity

        # If the successor has dependent sync predecessors,
        # we need to inform the platform that the function has finished.
        for instance in workflow_placement_decision["instances"].values():
            if instance["instance_name"] == successor_instance_name and "dependent_sync_predecessors" in instance:
                for predecessor_and_sync in instance.get("dependent_sync_predecessors", []):
                    predecessor = predecessor_and_sync[0]
                    sync_node = predecessor_and_sync[1]

                    response_size, consumed_capacity = self._inform_and_invoke_sync_node(
                        workflow_placement_decision, sync_node, predecessor, sync_nodes_invoked_logs
                    )

                    total_sync_data_response_size += response_size
                    total_consumed_capacity += consumed_capacity

                break

        return total_sync_data_response_size, total_consumed_capacity, sync_nodes_invoked_logs

    def _inform_and_invoke_sync_node(
        self,
        workflow_placement_decision: dict[str, Any],
        successor_instance_name: str,
        predecessor_instance_name: str,
        sync_nodes_invoked_logs: list[dict[str, Any]],
    ) -> tuple[float, float]:
        potential_call_start_time = datetime.now(GLOBAL_TIME_ZONE)

        # Total consumed write capacity
        total_consumed_write_capacity = 0.0
        total_sync_data_response_size = 0.0

        provider, region, identifier = self.get_successor_workflow_placement_decision(
            successor_instance_name, workflow_placement_decision
        )

        expected_counter = len(
            set(
                workflow_placement_decision.get("instances", {})
                .get(successor_instance_name, {})
                .get("preceding_instances", [])
            )
        )

        reached_states, response_size, consumed_write_capacity = RemoteClientFactory.get_remote_client(
            provider, region
        ).set_predecessor_reached(
            predecessor_name=predecessor_instance_name,
            sync_node_name=successor_instance_name,
            workflow_instance_id=workflow_placement_decision["run_id"],
            direct_call=False,
        )
        total_sync_data_response_size += response_size
        total_consumed_write_capacity += consumed_write_capacity

        # If all the predecessors have been reached and any of them have directly reached the sync node
        # then we can call the sync node
        if len(reached_states) == expected_counter and any(reached_states):
            successor_workflow_placement_decision = self.get_successor_workflow_placement_decision_dictionary(
                workflow_placement_decision, successor_instance_name
            )
            payload_wrapper: dict[str, Any] = {}
            transmission_taint = uuid.uuid4().hex
            payload_wrapper["workflow_placement_decision"] = successor_workflow_placement_decision
            payload_wrapper["transmission_taint"] = transmission_taint
            json_payload = json.dumps(payload_wrapper)

            _, _, _, _, _ = RemoteClientFactory.get_remote_client(provider, region).invoke_function(
                message=json_payload,
                identifier=identifier,
                workflow_instance_id=workflow_placement_decision["run_id"],
                sync=False,
            )

            sync_nodes_invoked_logs.append(
                {
                    "sync_node": successor_instance_name,
                    "predecessor": predecessor_instance_name,
                    "payload_size": len(json_payload.encode("utf-8")) / (1024**3),
                    "transmission_taint": transmission_taint,
                    "region": region,
                    "provider": provider,
                    "call_start_time": potential_call_start_time,
                    "finish_time": datetime.now(GLOBAL_TIME_ZONE),
                }
            )

        # FOR DEBUG ONLY -> can be safely removed if needed
        log_message = (
            f"INFORMING_SYNC_NODE: INSTANCE ({predecessor_instance_name}) informing "
            f"SYNC_NODE ({successor_instance_name}) of non-execution"
        )
        self.log_for_retrieval(
            log_message,
            workflow_placement_decision["run_id"],
        )

        return total_sync_data_response_size, total_consumed_write_capacity

    def get_successor_workflow_placement_decision(
        self, successor_instance_name: str, workflow_placement_decision: dict[str, Any]
    ) -> tuple[str, str, str]:
        if (
            "send_to_home_region" in workflow_placement_decision and workflow_placement_decision["send_to_home_region"]
        ) or ("current_deployment" not in workflow_placement_decision["workflow_placement"]):
            provider_region = workflow_placement_decision["workflow_placement"]["home_deployment"][
                successor_instance_name
            ]["provider_region"]
            identifier = workflow_placement_decision["workflow_placement"]["home_deployment"][successor_instance_name][
                "identifier"
            ]
        else:
            provider_region = workflow_placement_decision["workflow_placement"]["current_deployment"]["instances"][
                workflow_placement_decision["time_key"]
            ][successor_instance_name]["provider_region"]
            identifier = workflow_placement_decision["workflow_placement"]["current_deployment"]["instances"][
                workflow_placement_decision["time_key"]
            ][successor_instance_name]["identifier"]

        return provider_region["provider"], provider_region["region"], identifier

    # This method is used to get the name of the next successor instance and its workflow_placement decision.
    def get_successor_instance_name(
        self,
        function: Callable[..., Any],
        workflow_placement_decision: dict[str, Any],
    ) -> tuple[str, dict[str, Any]]:
        # Get the name of the successor function
        successor_function_name = self.functions[function.original_function.__name__].name  # type: ignore

        # Set the current instance name based on whether it is the entry point or not
        current_instance_name = workflow_placement_decision["current_instance_name"]

        # Get the name of the next instance based on the current instance and successor function name
        next_instance_name = self.get_next_instance_name(
            current_instance_name, workflow_placement_decision, successor_function_name
        )

        # Create the successor workflow_placement decision by copying the original workflow_placement decision
        # and updating the current instance name to the next instance name
        successor_workflow_placement_decision = self.get_successor_workflow_placement_decision_dictionary(
            workflow_placement_decision, next_instance_name
        )

        # Return the next instance name and the successor workflow_placement decision
        return next_instance_name, successor_workflow_placement_decision

    def get_successor_workflow_placement_decision_dictionary(
        self, workflow_placement_decision: dict[str, Any], next_instance_name: str
    ) -> dict[str, Any]:
        # Copy the workflow_placement decision
        successor_workflow_placement_decision = workflow_placement_decision.copy()
        # Update the current instance name to the next instance name
        successor_workflow_placement_decision["current_instance_name"] = next_instance_name
        return successor_workflow_placement_decision

    # This method is used to get the name of the next successor instance based on the current instance name,
    # workflow_placement decision, and successor function name.
    def get_next_instance_name(
        self, current_instance_name: str, workflow_placement_decision: dict[str, Any], successor_function_name: str
    ) -> str:
        # Get the workflow_placement decision for the current instance
        if current_instance_name not in workflow_placement_decision["instances"]:
            raise RuntimeError(
                f"Could not find current instance name {current_instance_name} in {workflow_placement_decision['instances']}"  # pylint: disable=line-too-long
            )
        instance = workflow_placement_decision["instances"][current_instance_name]
        successor_instances = instance["succeeding_instances"]
        # If there is only one successor instance, return it
        if len(successor_instances) == 1:
            if (
                successor_instances[0].split(":", maxsplit=1)[0]
                == f"{self.name}-{self.version.replace('.', '_')}-{successor_function_name}"
            ):
                return successor_instances[0]
            raise RuntimeError(
                f"Could not find successor instance for successor function name {successor_function_name} in {successor_instances}"  # pylint: disable=line-too-long
            )
        # If there are multiple successor instances, return the first one that matches the successor function
        # name and has the correct index
        for successor_instance in successor_instances:
            if (
                successor_instance.split(":", maxsplit=1)[0]
                == f"{self.name}-{self.version.replace('.', '_')}-{successor_function_name}"
            ):
                if successor_instance.split(":", maxsplit=2)[1] == "sync":
                    return successor_instance
                if successor_instance.split(":", maxsplit=2)[1].split("_")[-1] == str(
                    self._run_id_to_successor_index.get(workflow_placement_decision["run_id"], 0)
                ):
                    if workflow_placement_decision["run_id"] not in self._run_id_to_successor_index:
                        self._run_id_to_successor_index[workflow_placement_decision["run_id"]] = 0
                    self._run_id_to_successor_index[workflow_placement_decision["run_id"]] += 1
                    return successor_instance
        raise RuntimeError(
            f"Could not find successor instance for successor function name {successor_function_name} in {successor_instances}"  # pylint: disable=line-too-long
        )

    def get_workflow_placement_decision(self) -> dict[str, Any]:
        """
        The structure of the workflow placement decision is explained in the
        `docs/component_interaction.md` file under `Workflow Placement Decision`.
        """
        return self._current_workflow_placement_decision

    def get_predecessor_data(self) -> list[dict[str, Any]]:
        """
        Get the data returned by the predecessor functions.

        This method is only invoked if the sync function was
        called which means all predecessor functions have finished.
        """
        (
            provider,
            region,
            current_instance_name,
            workflow_instance_id,
        ) = self.get_current_instance_provider_region_instance_name()
        # log the start time of loading the data
        get_predecessor_start_time = datetime.now(GLOBAL_TIME_ZONE)

        client = RemoteClientFactory.get_remote_client(provider, region)

        response, consumed_capacity = client.get_predecessor_data(current_instance_name, workflow_instance_id)
        size_of_results = sum(len(message.encode("utf-8")) for message in response)
        results: list[dict[str, Any]] = [json.loads(message, cls=CustomDecoder) for message in response]

        # log the end time of loading the data
        get_predecessor_end_time = datetime.now(GLOBAL_TIME_ZONE)

        # Get the time taken to load the data
        get_predecessor_duration = (get_predecessor_end_time - get_predecessor_start_time).total_seconds()

        # Now log the loaded data (To show that the data was loaded from dynamodb)
        # MAY LOOK INTO LOGGING THE TIME OF THIS DEPENDENT ON DOWNLOAD SIZE
        log_message = (
            f"DOWNLOAD_DATA_FROM_SYNC_TABLE: INSTANCE ({current_instance_name}) loaded SYNC_NODE_PREDECESSOR_DATA "
            f"with DOWNLOAD_SIZE ({size_of_results / (1024**3)}) GB and "
            f"CONSUMED_READ_CAPACITY ({consumed_capacity}) and taken "
            f"DOWNLOAD_TIME ({get_predecessor_duration}) s"
        )
        self.log_for_retrieval(
            log_message,
            self.get_run_id(),
        )

        return results

    def get_current_instance_provider_region_instance_name(self) -> tuple[str, str, str, str]:
        workflow_placement_decision = self.get_workflow_placement_decision()

        if "current_instance_name" not in workflow_placement_decision:
            raise RuntimeError(
                "Could not get current instance name, is this the entry point? Entry point cannot be sync function"
            )

        if "run_id" not in workflow_placement_decision:
            raise RuntimeError(
                "Could not get workflow instance id, is this the entry point? Entry point cannot be sync function"
            )

        current_instance_name = workflow_placement_decision["current_instance_name"]
        workflow_instance_id = workflow_placement_decision["run_id"]

        key = self._get_deployment_key(workflow_placement_decision)

        if key == "home_deployment":
            provider_region = workflow_placement_decision["workflow_placement"][key][current_instance_name][
                "provider_region"
            ]
        else:
            time_key = workflow_placement_decision["time_key"]
            provider_region = workflow_placement_decision["workflow_placement"][key]["instances"][time_key][
                current_instance_name
            ]["provider_region"]
        return provider_region["provider"], provider_region["region"], current_instance_name, workflow_instance_id

    def _get_deployment_key(self, workflow_placement_decision: dict[str, Any]) -> str:
        key = "home_deployment"
        if workflow_placement_decision.get("send_to_home_region", False) or (
            "current_deployment" not in workflow_placement_decision["workflow_placement"]
        ):
            return key

        # Check if the deployment is not expired
        deployment_expiry_time = workflow_placement_decision["workflow_placement"]["current_deployment"]["expiry_time"]
        if deployment_expiry_time is not None:
            # If the deployment is expired, return the home deployment
            if datetime.now(GLOBAL_TIME_ZONE) <= datetime.strptime(deployment_expiry_time, TIME_FORMAT):
                key = "current_deployment"

        return key

    def get_workflow_placement_decision_from_platform(self) -> tuple[dict[str, Any], float, float]:
        """
        Get the workflow_placement decision from the platform.
        """
        (
            result,
            consumed_read_capacity,
        ) = self._endpoint.get_deployment_algorithm_workflow_placement_decision_client().get_value_from_table(
            WORKFLOW_PLACEMENT_DECISION_TABLE, f"{self.name}-{self.version}"
        )
        if result is not None:
            data_size = len(result.encode("utf-8")) / (1024**3)
            return json.loads(result), data_size, consumed_read_capacity

        raise RuntimeError("Could not get workflow_placement decision from platform")

    def register_function(  # pylint: disable=too-many-statements
        self,
        function: Callable[..., Any],
        name: str,
        entry_point: bool,
        regions_and_providers: dict,
        environment_variables: list[dict[str, str]],
    ) -> None:
        """
        Register a function as a serverless function.

        Where the function is deployed depends on the workflow_placement decision which will be made by the
        deployment algorithm.

        At this point we only need to register the function with the wrapper, the actual deployment will be done
        later by the deployment manager.
        """
        wrapper = CaribouFunction(function, name, entry_point, regions_and_providers, environment_variables)
        if function.__name__ in self.functions:
            raise RuntimeError(f"Function with function name {function.__name__} already registered")
        if name in self._function_names:
            raise RuntimeError(f"Function with given name {name} already registered")
        self._function_names.add(name)
        self.functions[function.__name__] = wrapper

    def serverless_function(  # pylint: disable=too-many-statements
        self,
        name: Optional[str] = None,
        entry_point: bool = False,
        regions_and_providers: Optional[dict] = None,
        environment_variables: Optional[list[dict[str, str]]] = None,
    ) -> Callable[..., Any]:
        """
        Decorator to register a function as a Lambda function.

        :param name: The name of the Lambda function. Defaults to the name of the function being decorated.
        :param entry_point: Whether this function is the entry point for the workflow.

        The following is mildly complicated, but it is necessary to make the decorator work.

        The three layers of functions are used to create a decorator with arguments.

        Outermost function (serverless_function):
            This is the decorator factory. It takes in arguments for the decorator and returns the actual
            decorator function.
            The arguments passed to this function are used to configure the behavior of the decorator.
            In this case, name, entry_point, regions_and_providers, and providers are used to configure
            the Lambda function.

        Middle function (_register_handler):
            This is the actual decorator function. It takes in a single argument, which is the function to be decorated.
            It returns a new function that wraps the original function and modifies its behavior.
            In this case, _register_handler takes in func, which is the function to be decorated, and returns wrapper,
            which is a new function that wraps func.
            The middle function is responsible for creating the wrapper function and returning it as well as registering
            the function with the workflow.

        Innermost function (wrapper):
            This is the wrapper function that modifies the behavior of the original function.
            It takes the same arguments as the original function and can modify these arguments
            before calling the original function.
            It can also modify the return value of the original function.
            In this case, wrapper unwraps the arguments of func and retrieves the workflow_placement decision
            and calls func with the modified unwrapped payload.
        """
        if regions_and_providers is None:
            regions_and_providers = {}

        if environment_variables is None:
            environment_variables = []
        else:
            if not isinstance(environment_variables, list):
                raise RuntimeError("environment_variables must be a list of dicts")
            for env_variable in environment_variables:
                if not isinstance(env_variable, dict):
                    raise RuntimeError("environment_variables must be a list of dicts")
                if "key" not in env_variable or "value" not in env_variable:
                    raise RuntimeError("environment_variables must be a list of dicts with keys 'key' and 'value'")
                if not isinstance(env_variable["key"], str):
                    raise RuntimeError("environment_variables must be a list of dicts with 'key' as a string")
                if not isinstance(env_variable["value"], str):
                    raise RuntimeError("environment_variables must be a list of dicts with 'value' as a string")

        def _register_handler(func: Callable[..., Any]) -> Callable[..., Any]:
            handler_name = name if name is not None else func.__name__

            def wrapper(*args, **kwargs):  # type: ignore  # pylint: disable=unused-argument, too-many-branches
                self._function_start_time = datetime.now(GLOBAL_TIME_ZONE)

                # Modify args and kwargs here as needed
                argument_raw = args[0]

                if (
                    "Records" in argument_raw
                    and len(argument_raw["Records"]) == 1
                    and "Sns" in argument_raw["Records"][0]
                    and "Message" in argument_raw["Records"][0]["Sns"]
                ):
                    argument = json.loads(argument_raw["Records"][0]["Sns"]["Message"], cls=CustomDecoder)
                else:
                    try:
                        argument = json.loads(argument_raw, cls=CustomDecoder)
                    except json.JSONDecodeError as e:
                        raise RuntimeError(
                            f"Could not get message from argument {argument_raw}, there should be meta information in the message"  # pylint: disable=line-too-long
                        ) from e

                if isinstance(argument, dict):
                    transmission_taint = argument.get("transmission_taint", "N/A")
                else:
                    transmission_taint = "N/A"

                if entry_point:
                    send_to_home_region = False
                    time_invoked_at_client = None
                    pre_loaded_workflow_placement_decision = None
                    wpd_data_size = 0.0
                    wpd_consumed_read_capacity = 0.0
                    run_id: Optional[str] = None
                    if isinstance(argument, dict) and "workflow_placement_decision" in argument:
                        time_invoked_at_client = argument.get("time_request_sent", None)
                        pre_loaded_workflow_placement_decision = argument.get("workflow_placement_decision", None)
                        wpd_data_size = argument.get("wpd_data_size", 0.0)
                        wpd_consumed_read_capacity = argument.get("wpd_consumed_read_capacity", 0.0)
                        send_to_home_region = pre_loaded_workflow_placement_decision.get("send_to_home_region", False)
                        run_id = pre_loaded_workflow_placement_decision.get("run_id", None)
                        argument = argument.get("input_data", {})

                    init_latency = "N/A"
                    if time_invoked_at_client:
                        datetime_invoked_at_client = datetime.strptime(time_invoked_at_client, TIME_FORMAT)
                        time_difference_datetime = self._function_start_time - datetime_invoked_at_client
                        # Get s from the time difference
                        # Note due to desync between client and server, the time difference can be negative
                        # In this case, we set the init_latency to 0.
                        init_latency = str(max(time_difference_datetime.total_seconds(), 0.0))

                    if pre_loaded_workflow_placement_decision:
                        workflow_placement_decision = pre_loaded_workflow_placement_decision
                    else:
                        (
                            workflow_placement_decision,
                            data_size,
                            consumed_read_capacity,
                        ) = self.get_workflow_placement_decision_from_platform()  # pylint: disable=line-too-long
                        wpd_data_size = data_size
                        wpd_consumed_read_capacity = consumed_read_capacity

                    # This is the first function to be called, so we need to generate a run id
                    # If this run ID has not already been generated
                    # This run id will be used to identify the workflow instance
                    if run_id is None:
                        run_id = uuid.uuid4().hex

                    workflow_placement_decision["run_id"] = run_id
                    workflow_placement_decision["send_to_home_region"] = send_to_home_region
                    workflow_placement_decision["time_key"] = self._get_time_key(workflow_placement_decision)

                    # Log the retrieval of workflow placement decision
                    payload = argument
                    log_message = (
                        f"ENTRY_POINT: Entry Point INSTANCE "
                        f'({workflow_placement_decision["current_instance_name"]}) '
                        f'of workflow {f"{self.name}-{self.version}"} called with PAYLOAD_SIZE '
                        f'({len(json.dumps(payload, cls=CustomEncoder).encode("utf-8")) / (1024**3)}) GB and '
                        f"INIT_LATENCY ({init_latency}) s with WORKFLOW_PLACEMENT_DECISION_SIZE "
                        f"({wpd_data_size}) GB and CONSUMED_READ_CAPACITY ({wpd_consumed_read_capacity})"
                    )
                    self.log_for_retrieval(
                        log_message, workflow_placement_decision["run_id"], self._function_start_time
                    )
                else:
                    # Get the workflow_placement decision from the message received from the predecessor function
                    if "workflow_placement_decision" not in argument:
                        raise RuntimeError("Could not get workflow_placement decision from message")
                    workflow_placement_decision = argument["workflow_placement_decision"]
                    payload = argument.get("payload", {})

                log_message = (
                    f'INVOKED: INSTANCE ({workflow_placement_decision["current_instance_name"]}) '
                    f"called with TAINT ({transmission_taint})"
                )
                self.log_for_retrieval(log_message, workflow_placement_decision["run_id"], self._function_start_time)

                self._current_workflow_placement_decision = workflow_placement_decision
                self._run_id_to_successor_index[workflow_placement_decision["run_id"]] = 0

                cpu_model = self.get_cpu_info()
                log_message = (
                    f"USED_CPU_MODEL: CPU_MODEL ({cpu_model.replace('(', '<').replace(')', '>')}) used in INSTANCE "
                    f'({workflow_placement_decision["current_instance_name"]})'
                )
                self.log_for_retrieval(
                    log_message,
                    workflow_placement_decision["run_id"],  # type: ignore
                    self._function_start_time,
                )

                result: Any = None
                try:
                    # Call the function with the payload and the caribou metadata
                    result = func(payload)

                    user_code_end_time = datetime.now(GLOBAL_TIME_ZONE)

                    # Wait until all the futures (Invoke serverless functions) are done
                    for future in self._futures:
                        future.result()
                    self._futures = []

                    end_time = datetime.now(GLOBAL_TIME_ZONE)

                    user_execution_time = (user_code_end_time - self._function_start_time).total_seconds()
                    log_message = (
                        f'EXECUTED: INSTANCE ({workflow_placement_decision["current_instance_name"]}) with '
                        f"USER_EXECUTION_TIME ({user_execution_time}) s and "
                        f"TOTAL_EXECUTION_TIME ({(end_time - self._function_start_time).total_seconds()}) s"
                    )
                    self.log_for_retrieval(
                        log_message,
                        workflow_placement_decision["run_id"],
                    )
                except Exception as e:  # pylint: disable=broad-except
                    log_message = (
                        f'EXCEPTION: INSTANCE ({workflow_placement_decision["current_instance_name"]}) '
                        f"raised EXCEPTION ({e})"
                    )
                    self.log_for_retrieval(
                        log_message,
                        workflow_placement_decision["run_id"],
                    )

                    # TODO: Add mechanism to log failures of a workflow with its run_id

                    # Raise the error now
                    # To terminate the lambda function
                    raise e

                return result

            wrapper.original_function = func  # type: ignore
            self.register_function(func, handler_name, entry_point, regions_and_providers, environment_variables)
            return wrapper

        return _register_handler

    def get_caribou_metadata(self) -> dict[str, Any]:
        cpu_model = self.get_cpu_info()
        (
            provider,
            region,
            current_instance_name,
            workflow_instance_id,
        ) = self.get_current_instance_provider_region_instance_name()

        caribou_metadata = {
            "workflow_name": f"{self.name}-{self.version}",
            "run_id": workflow_instance_id,
            "current_instance_name": current_instance_name,
            "current_provider": provider,
            "current_region": region,
            "cpu_model": cpu_model,
            "function_start_time": self._function_start_time.strftime(TIME_FORMAT),
        }

        return caribou_metadata

    def log_for_retrieval(self, message: str, run_id: str, message_time: Optional[datetime] = None) -> None:
        """
        Log a message for retrieval by the platform.
        """
        if message_time is None:
            message_time = datetime.now(GLOBAL_TIME_ZONE)
        message_time_str: str = message_time.strftime(TIME_FORMAT)

        logger.caribou(
            "TIME (%s) RUN_ID (%s) MESSAGE (%s) LOG_VERSION (%s)",
            message_time_str,
            run_id,
            message,
            LOG_VERSION,
        )

    def _get_time_key(self, workflow_placement_decision: dict[str, Any]) -> str:
        if "time_key" in workflow_placement_decision:
            return workflow_placement_decision["time_key"]
        if "current_deployment" not in workflow_placement_decision["workflow_placement"]:
            return "N/A"

        all_time_keys = workflow_placement_decision["workflow_placement"]["current_deployment"]["time_keys"]

        current_hour_of_day = datetime.now(GLOBAL_TIME_ZONE).hour

        previous_time_key = max(time_key for time_key in all_time_keys if int(time_key) <= current_hour_of_day)

        return previous_time_key

    def get_cpu_info(self) -> str:
        # Log the CPU model, workflow name, and the request ID
        cpu_model = ""
        with open("/proc/cpuinfo", encoding="utf-8") as f:
            for line in f:
                if "model name" in line:
                    cpu_model = line.split(":")[1].strip()  # Extracting and cleaning the model name
                    break  # No need to continue the loop once the model name is found
        return cpu_model
