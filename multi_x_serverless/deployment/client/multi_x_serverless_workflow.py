from __future__ import annotations

import inspect
import json
import logging
import re
import time
import uuid
from datetime import datetime
from types import FrameType
from typing import Any, Callable, Optional

from multi_x_serverless.common.constants import (
    GLOBAL_TIME_ZONE,
    LOG_VERSION,
    TIME_FORMAT,
    WORKFLOW_PLACEMENT_DECISION_TABLE,
)
from multi_x_serverless.common.models.endpoints import Endpoints
from multi_x_serverless.common.models.remote_client.remote_client_factory import RemoteClientFactory
from multi_x_serverless.common.utils import get_function_source
from multi_x_serverless.deployment.client.multi_x_serverless_function import MultiXServerlessFunction

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())


class MultiXServerlessWorkflow:
    """
    MultiXServerlessWorkflow class that is used to register functions as a collection of connected serverless functions.

    Every workflow must have an instance of this class.
    The instance is used to register functions as serverless functions.

    Every workflow must have one function that is the entry point for the workflow. This function must be registered
    with the `serverless_function` decorator with the `entry_point` parameter set to `True`.

    :param name: The name of the workflow.
    """

    def __init__(self, name: str, version: str):
        self.name = name
        self.version = version
        self.functions: dict[str, MultiXServerlessFunction] = {}
        self._successor_index = 0
        self._function_names: set[str] = set()
        self._endpoint = Endpoints()

    def _verify_name_and_version(self) -> None:
        # Both colon and minus are not allowed in the name and version
        if ":" in self.name or "-" in self.name:
            raise RuntimeError("Name cannot contain colon or minus")
        if ":" in self.version or "-" in self.version:
            raise RuntimeError("Version cannot contain colon or minus")

    def get_wrapper_frame(self, current_frame: Optional[FrameType]) -> FrameType:
        if not current_frame:
            raise RuntimeError("Could not get current frame")

        frame = current_frame

        while "wrapper" not in frame.f_locals:
            frame = frame.f_back  # type: ignore
            if not frame:
                raise RuntimeError("Could not get wrapper frame")

        return frame

    def get_successors(self, function: MultiXServerlessFunction) -> list[MultiXServerlessFunction]:
        """
        Get the functions that are called by this function.
        """
        source_code = get_function_source(function.function_callable)
        function_calls = re.findall(r"invoke_serverless_function\((.*?)\)", source_code)
        successors: list[MultiXServerlessFunction] = []
        for call in function_calls:
            function_name = None
            if "," not in call:
                function_name = call.strip().strip('"')
            else:
                function_name = call.strip().split(",", maxsplit=1)[0].strip('"')
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

        # We need to go back two frames to get the frame of the wrapper function that stores
        # the workflow_placement decision
        # and the payload (see more on this in the explanation of the decorator `serverless_function`)
        wrapper_frame = self.get_wrapper_frame(inspect.currentframe())

        workflow_placement_decision = self.get_workflow_placement_decision(wrapper_frame)

        current_instance_name = workflow_placement_decision["current_instance_name"]

        successor_instance_name, successor_workflow_placement_decision_dictionary = self.get_successor_instance_name(
            function, workflow_placement_decision
        )

        if not conditional:
            # We don't call the function if it is conditional and the condition is not met.
            log_message = (
                f"CONDITIONAL_NON_EXECUTION: INSTANCE ({current_instance_name}) calling "
                f"SUCCESSOR ({successor_instance_name})"
            )
            self.log_for_retrieval(
                log_message,
                workflow_placement_decision["run_id"],
            )

            # We still need to increment the successor index, because the next function might not be conditional.
            self._successor_index += 1

            # However, for the sync nodes we still need to inform the platform that the function has finished.
            self._inform_sync_node_of_conditional_non_execution(
                workflow_placement_decision, successor_instance_name, current_instance_name
            )
            return

        # Wrap the payload and add the workflow_placement decision
        payload_wrapper = {}
        if payload:
            payload_wrapper["payload"] = payload

        transmission_taint = uuid.uuid4().hex

        payload_wrapper["workflow_placement_decision"] = successor_workflow_placement_decision_dictionary
        payload_wrapper["transmission_taint"] = transmission_taint
        json_payload = json.dumps(payload_wrapper)

        provider, region, identifier = self.get_successor_workflow_placement_decision(
            successor_instance_name, workflow_placement_decision
        )

        is_successor_sync_node = successor_instance_name.split(":", maxsplit=2)[1] == "sync"

        expected_counter = -1
        if is_successor_sync_node:
            expected_counter = len(
                set(workflow_placement_decision["instances"][successor_instance_name]["preceding_instances"])
            )

        log_message = (
            f"INVOKING_SUCCESSOR: INSTANCE ({current_instance_name}) calling "
            f"SUCCESSOR ({successor_instance_name}) with PAYLOAD_SIZE "
            f"({len(json_payload.encode('utf-8')) / (1024**3)}) GB and TAINT ({transmission_taint})"
        )
        self.log_for_retrieval(
            log_message,
            workflow_placement_decision["run_id"],
        )

        RemoteClientFactory.get_remote_client(provider, region).invoke_function(
            message=json_payload,
            identifier=identifier,
            workflow_instance_id=workflow_placement_decision["run_id"],
            sync=is_successor_sync_node,
            function_name=successor_instance_name,
            expected_counter=expected_counter,
            current_instance_name=current_instance_name,
        )

    def _inform_sync_node_of_conditional_non_execution(
        self, workflow_placement_decision: dict[str, Any], successor_instance_name: str, current_instance_name: str
    ) -> None:
        # If the successor is a sync node, we need to inform the platform that the function has finished.
        if successor_instance_name.split(":", maxsplit=2)[1] == "sync":
            self._inform_and_invoke_sync_node(
                workflow_placement_decision, successor_instance_name, current_instance_name
            )

        # If the successor has dependent sync predecessors,
        # we need to inform the platform that the function has finished.
        for instance in workflow_placement_decision["instances"].values():
            if instance["instance_name"] == successor_instance_name and "dependent_sync_predecessors" in instance:
                for predecessor_and_sync in instance.get("dependent_sync_predecessors", []):
                    predecessor = predecessor_and_sync[0]
                    sync_node = predecessor_and_sync[1]

                    self._inform_and_invoke_sync_node(workflow_placement_decision, sync_node, predecessor)
                break

    def _inform_and_invoke_sync_node(
        self, workflow_placement_decision: dict[str, Any], successor_instance_name: str, predecessor_instance_name: str
    ) -> None:
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

        reached_states = RemoteClientFactory.get_remote_client(provider, region).set_predecessor_reached(
            predecessor_name=predecessor_instance_name,
            sync_node_name=successor_instance_name,
            workflow_instance_id=workflow_placement_decision["run_id"],
            direct_call=False,
        )

        log_message = (
            f"INFORMING_SYNC_NODE: INSTANCE ({predecessor_instance_name}) informing "
            f"SYNC_NODE ({successor_instance_name}) of non-execution"
        )
        self.log_for_retrieval(
            log_message,
            workflow_placement_decision["run_id"],
        )

        # If all the predecessors have been reached and any of them have directly reached the sync node
        # then we can call the sync node

        if len(reached_states) == expected_counter and any(reached_states):
            successor_workflow_placement_decision = self.get_successor_workflow_placement_decision_dictionary(
                workflow_placement_decision, successor_instance_name
            )
            payload_wrapper = {}
            payload_wrapper["workflow_placement_decision"] = successor_workflow_placement_decision
            json_payload = json.dumps(payload_wrapper)

            RemoteClientFactory.get_remote_client(provider, region).invoke_function(
                message=json_payload,
                identifier=identifier,
                workflow_instance_id=workflow_placement_decision["run_id"],
                sync=False,
            )

    def get_successor_workflow_placement_decision(
        self, successor_instance_name: str, workflow_placement_decision: dict[str, Any]
    ) -> tuple[str, str, str]:
        if "send_to_home_region" in workflow_placement_decision and workflow_placement_decision["send_to_home_region"]:
            key = "home_deployment"
        else:
            key = "current_deployment"
        provider_region = workflow_placement_decision["workflow_placement"][key]["instances"][successor_instance_name][
            "provider_region"
        ]
        identifier = workflow_placement_decision["workflow_placement"][key]["instances"][successor_instance_name][
            "identifier"
        ]
        return provider_region["provider"], provider_region["region"], identifier

    # This method is used to get the name of the next successor instance and its workflow_placement decision.
    # It takes the current function, workflow_placement decision, wrapper frame, and function frame as parameters.
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
            raise RuntimeError(f"Could not find current instance name {current_instance_name} in {workflow_placement_decision['instances']}")  # type: ignore  # pylint: disable=line-too-long
        instance = workflow_placement_decision["instances"][current_instance_name]
        successor_instances = instance["succeeding_instances"]
        # If there is only one successor instance, return it
        if len(successor_instances) == 1:
            if (
                successor_instances[0].split(":", maxsplit=1)[0]
                == f"{self.name}-{self.version.replace('.', '_')}-{successor_function_name}"
            ):
                return successor_instances[0]
            raise RuntimeError(f"Could not find successor instance for successor function name {successor_function_name} in {successor_instances}")  # type: ignore  # pylint: disable=line-too-long
        # If there are multiple successor instances, return the first one that matches the successor function
        # name and has the correct index
        for successor_instance in successor_instances:
            if (
                successor_instance.split(":", maxsplit=1)[0]
                == f"{self.name}-{self.version.replace('.', '_')}-{successor_function_name}"
            ):
                if successor_instance.split(":", maxsplit=2)[1] == "sync":
                    return successor_instance
                if successor_instance.split(":", maxsplit=2)[1].split("_")[-1] == str(self._successor_index):
                    self._successor_index += 1
                    return successor_instance
        raise RuntimeError(f"Could not find successor instance for successor function name {successor_function_name} in {successor_instances}")  # type: ignore  # pylint: disable=line-too-long

    def get_workflow_placement_decision(self, frame: FrameType) -> dict[str, Any]:
        """
        The structure of the workflow placement decision is explained in the
        `docs/design.md` file under `Workflow Placement Decision`.
        """
        # Get the workflow_placement decision from the wrapper function
        while "wrapper" not in frame.f_locals:
            frame = frame.f_back  # type: ignore
            if not frame:
                raise RuntimeError("Could not get workflow_placement decision")
        wrapper = frame.f_locals["wrapper"]
        if hasattr(wrapper, "workflow_placement_decision"):
            return wrapper.workflow_placement_decision

        raise RuntimeError("Could not get workflow_placement decision")

    def is_entry_point(self, frame: FrameType) -> bool:
        # Check if the function is the entry point
        if "wrapper" in frame.f_locals:
            wrapper = frame.f_locals["wrapper"]
            if hasattr(wrapper, "entry_point"):
                return wrapper.entry_point
            return False

        raise RuntimeError("Could not get entry point")

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

        client = RemoteClientFactory.get_remote_client(provider, region)

        response = client.get_predecessor_data(current_instance_name, workflow_instance_id)

        return [json.loads(message) for message in response]

    def get_current_instance_provider_region_instance_name(self) -> tuple[str, str, str, str]:
        # We need to go back two frames to get the frame of the wrapper function that
        # stores the workflow_placement decision.
        # and the payload (see more on this in the explanation of the decorator `serverless_function`)
        wrapper_frame = self.get_wrapper_frame(inspect.currentframe())

        workflow_placement_decision = self.get_workflow_placement_decision(wrapper_frame)

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

        provider_region = workflow_placement_decision["workflow_placement"][key]["instances"][current_instance_name][
            "provider_region"
        ]
        return provider_region["provider"], provider_region["region"], current_instance_name, workflow_instance_id

    def _get_deployment_key(self, workflow_placement_decision: dict[str, Any]) -> str:
        key = "home_deployment"
        if workflow_placement_decision.get("send_to_home_region", False):
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

    def get_workflow_placement_decision_from_platform(self) -> dict[str, Any]:
        """
        Get the workflow_placement decision from the platform.
        """
        result = self._endpoint.get_deployment_algorithm_workflow_placement_decision_client().get_value_from_table(
            WORKFLOW_PLACEMENT_DECISION_TABLE, f"{self.name}-{self.version}"
        )
        if result is not None:
            return json.loads(result)

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
        wrapper = MultiXServerlessFunction(function, name, entry_point, regions_and_providers, environment_variables)
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

            def wrapper(*args, **kwargs):  # type: ignore # pylint: disable=unused-argument
                # Modify args and kwargs here as needed
                argument_raw = args[0]

                if (
                    "Records" in argument_raw
                    and len(argument_raw["Records"]) == 1
                    and "Sns" in argument_raw["Records"][0]
                    and "Message" in argument_raw["Records"][0]["Sns"]
                ):
                    argument = json.loads(argument_raw["Records"][0]["Sns"]["Message"])
                else:
                    try:
                        argument = json.loads(argument_raw)
                    except json.JSONDecodeError as e:
                        raise RuntimeError(
                            f"Could not get message from argument {argument_raw}, there should be meta information in the message"  # pylint: disable=line-too-long
                        ) from e

                transmission_taint = argument.get("transmission_taint", "N/A")

                if entry_point:
                    send_to_home_region = False
                    time_invoked_at_client = None
                    pre_loaded_workflow_placement_decision = None
                    if isinstance(argument, dict) and "workflow_placement_decision" in argument:
                        time_invoked_at_client = argument.get("time_request_sent", None)
                        pre_loaded_workflow_placement_decision = argument.get("workflow_placement_decision", None)
                        send_to_home_region = pre_loaded_workflow_placement_decision.get("send_to_home_region", False)
                        argument = argument.get("input_data", {})

                    init_latency = "N/A"
                    if time_invoked_at_client:
                        datetime_invoked_at_client = datetime.strptime(time_invoked_at_client, TIME_FORMAT)
                        datetime_now = datetime.now(GLOBAL_TIME_ZONE)
                        time_difference_datetime = datetime_now - datetime_invoked_at_client
                        # Get s from the time difference
                        init_latency = str(time_difference_datetime.total_seconds())
                    if pre_loaded_workflow_placement_decision:
                        wrapper.workflow_placement_decision = pre_loaded_workflow_placement_decision  # type: ignore
                    else:
                        wrapper.workflow_placement_decision = self.get_workflow_placement_decision_from_platform()  # type: ignore  # pylint: disable=line-too-long
                    # This is the first function to be called, so we need to generate a run id
                    # This run id will be used to identify the workflow instance
                    wrapper.workflow_placement_decision["run_id"] = uuid.uuid4().hex  # type: ignore
                    wrapper.workflow_placement_decision["send_to_home_region"] = send_to_home_region  # type: ignore
                    if len(args) == 0:
                        return func()
                    payload = argument

                    log_message = (
                        f"ENTRY_POINT: : Entry Point INSTANCE "
                        f'({wrapper.workflow_placement_decision["current_instance_name"]}) '  # type: ignore
                        f'of workflow {f"{self.name}-{self.version}"} called with PAYLOAD_SIZE '
                        f'({len(json.dumps(payload).encode("utf-8")) / (1024**3)}) GB and '
                        f"INIT_LATENCY ({init_latency}) s"
                    )
                    self.log_for_retrieval(
                        log_message,
                        wrapper.workflow_placement_decision["run_id"],  # type: ignore
                    )
                else:
                    # Get the workflow_placement decision from the message received from the predecessor function
                    if "workflow_placement_decision" not in argument:
                        raise RuntimeError("Could not get workflow_placement decision from message")
                    wrapper.workflow_placement_decision = argument["workflow_placement_decision"]  # type: ignore
                    if "payload" not in argument:
                        return func({})
                    payload = argument.get("payload", {})

                log_message = (
                    f'INVOKED: INSTANCE ({wrapper.workflow_placement_decision["current_instance_name"]}) '  # type: ignore  # pylint: disable=line-too-long
                    f"called with TAINT ({transmission_taint})"
                )
                self.log_for_retrieval(
                    log_message,
                    wrapper.workflow_placement_decision["run_id"],  # type: ignore
                )

                # Call the original function with the modified arguments
                start_time = time.time()
                result = func(payload)
                end_time = time.time()

                log_message = (
                    f'EXECUTED: INSTANCE ({wrapper.workflow_placement_decision["current_instance_name"]}) '  # type: ignore  # pylint: disable=line-too-long
                    f"executed EXECUTION_TIME ({end_time - start_time})"
                )
                self.log_for_retrieval(
                    log_message,
                    wrapper.workflow_placement_decision["run_id"],  # type: ignore
                )

                return result

            wrapper.workflow_placement_decision = {}  # type: ignore
            wrapper.entry_point = entry_point  # type: ignore
            wrapper.original_function = func  # type: ignore
            self.register_function(func, handler_name, entry_point, regions_and_providers, environment_variables)
            return wrapper

        return _register_handler

    def log_for_retrieval(self, message: str, run_id: str) -> None:
        """
        Log a message for retrieval by the platform.
        """
        logger.info(
            "TIME (%s) RUN_ID (%s) MESSAGE (%s) LOG_VERSION (%s)",
            datetime.now(GLOBAL_TIME_ZONE).strftime(TIME_FORMAT),
            run_id,
            message,
            LOG_VERSION,
        )
