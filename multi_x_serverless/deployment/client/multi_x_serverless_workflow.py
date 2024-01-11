from __future__ import annotations

import inspect
import json
import re
import uuid
from types import FrameType
from typing import Any, Callable, Optional

from multi_x_serverless.deployment.client.multi_x_serverless_function import MultiXServerlessFunction
from multi_x_serverless.deployment.common.factories.remote_client_factory import RemoteClientFactory


class MultiXServerlessWorkflow:
    """
    MultiXServerlessWorkflow class that is used to register functions as a collection of connected serverless functions.

    Every workflow must have an instance of this class.
    The instance is used to register functions as serverless functions.

    Every workflow must have one function that is the entry point for the workflow. This function must be registered
    with the `serverless_function` decorator with the `entry_point` parameter set to `True`.

    :param name: The name of the workflow.
    """

    def __init__(self, name: str):
        self.name = name
        self.functions: dict[str, MultiXServerlessFunction] = {}
        self._successor_index = 0
        self._function_names: set[str] = set()

    def get_function_and_wrapper_frame(self, current_frame: Optional[FrameType]) -> tuple[FrameType, FrameType]:
        if not current_frame:
            raise RuntimeError("Could not get current frame")
        # Get the frame of the function
        function_frame = current_frame.f_back
        if not function_frame:
            raise RuntimeError("Could not get previous frame")

        # Get the frame of the wrapper function
        wrapper_frame = function_frame.f_back
        if not wrapper_frame:
            raise RuntimeError("Could not get previous frame")

        return function_frame, wrapper_frame

    def get_successors(self, function: MultiXServerlessFunction) -> list[MultiXServerlessFunction]:
        """
        Get the functions that are called by this function.
        """
        source_code = inspect.getsource(function.function_callable)
        function_calls = re.findall(r"invoke_serverless_function\((.*?)\)", source_code)
        successors: list[MultiXServerlessFunction] = []
        for call in function_calls:
            if "," not in call:
                raise RuntimeError(
                    f"""Could not parse function call ({call}) in function
                    ({function}), did you provide the payload?"""
                )
            function_name = call.strip().split(",", maxsplit=1)[0].strip('"')
            successor = next(
                (func for func in self.functions.values() if func.function_callable.__name__ == function_name), None
            )
            if successor:
                successors.append(successor)
        return successors

    def invoke_serverless_function(
        self, function: Callable[..., Any], payload: Optional[dict | Any] = None, conditional: bool = True
    ) -> None:
        """
        Invoke a serverless function which is part of this workflow.
        """
        if not payload:
            payload = {}

        if not conditional:
            return
        # If the function from which this function is called is the entry point obtain current routing decision
        # If not, the routing decision was stored in the message received from the predecessor function
        # Post message to SNS -> return
        # Do not wait for response

        # We need to go back two frames to get the frame of the wrapper function that stores the routing decision
        # and the payload (see more on this in the explanation of the decorator `serverless_function`)
        function_frame, wrapper_frame = self.get_function_and_wrapper_frame(inspect.currentframe())

        routing_decision = self.get_routing_decision(wrapper_frame)

        successor_instance_name, successor_routing_decision_dictionary = self.get_successor_instance_name(
            function, routing_decision, wrapper_frame, function_frame
        )

        # Wrap the payload and add the routing decision
        payload_wrapper = {"payload": payload}

        payload_wrapper["routing_decision"] = successor_routing_decision_dictionary
        json_payload = json.dumps(payload_wrapper)

        provider_region, identifier = self.get_successor_routing_decision(successor_instance_name, routing_decision)
        provider, region = provider_region.split(":")

        merge = successor_instance_name.split(":", maxsplit=2)[1] == "merge"

        expected_counter = -1
        function_name = None
        if merge:
            for instance in routing_decision["instances"]:
                if instance["instance_name"] == successor_instance_name:
                    expected_counter = len(instance["preceding_instances"])
                    break
            function_name = successor_instance_name.split(":", maxsplit=1)[0]

        RemoteClientFactory.get_remote_client(provider, region).invoke_function(
            message=json_payload,
            identifier=identifier,
            workflow_instance_id=routing_decision["run_id"],
            merge=merge,
            function_name=function_name,
            expected_counter=expected_counter,
        )

    def get_successor_routing_decision(
        self, successor_instance_name: str, routing_decision: dict[str, Any]
    ) -> tuple[str, str]:
        provider_region = routing_decision["routing_placement"][successor_instance_name]["provider_region"]
        identifier = routing_decision["routing_placement"][successor_instance_name]["identifier"]
        return provider_region, identifier

    # This method is used to get the name of the next successor instance and its routing decision.
    # It takes the current function, routing decision, wrapper frame, and function frame as parameters.
    def get_successor_instance_name(
        self,
        function: Callable[..., Any],
        routing_decision: dict[str, Any],
        wrapper_frame: FrameType,
        function_frame: FrameType,
    ) -> tuple[str, dict[str, Any]]:
        # Get the name of the successor function
        successor_function_name = self.functions[function.original_function.__name__].name  # type: ignore

        # Get the name of the current function
        current_function_name = self.functions[self.get_function__name__from_frame(function_frame)].name

        # Check if the current function is the entry point
        is_entry_point = self.is_entry_point(wrapper_frame)

        # Set the current instance name based on whether it is the entry point or not
        if is_entry_point:
            current_instance_name = f"{current_function_name}:entry_point:0"
        else:
            current_instance_name = routing_decision["current_instance_name"]

        # Get the name of the next instance based on the current instance and successor function name
        next_instance_name = self.get_next_instance_name(
            current_instance_name, routing_decision, successor_function_name
        )

        # Create the successor routing decision by copying the original routing decision
        # and updating the current instance name to the next instance name
        successor_routing_decision = self.get_successor_routing_decision_dictionary(
            routing_decision, next_instance_name
        )

        # Return the next instance name and the successor routing decision
        return next_instance_name, successor_routing_decision

    def get_successor_routing_decision_dictionary(
        self, routing_decision: dict[str, Any], next_instance_name: str
    ) -> dict[str, Any]:
        # Copy the routing decision
        successor_routing_decision = routing_decision.copy()
        # Update the current instance name to the next instance name
        successor_routing_decision["current_instance_name"] = next_instance_name
        return successor_routing_decision

    # This method is used to get the name of the next successor instance based on the current instance name,
    # routing decision, and successor function name.
    def get_next_instance_name(
        self, current_instance_name: str, routing_decision: dict[str, Any], successor_function_name: str
    ) -> str:
        # Get the routing decision for the current instance
        for instance in routing_decision["instances"]:
            if instance["instance_name"] == current_instance_name:
                successor_instances = instance["succeeding_instances"]
                # If there is only one successor instance, return it
                if len(successor_instances) == 1:
                    if successor_instances[0].startswith(successor_function_name):
                        return successor_instances[0]
                    raise RuntimeError("Could not find successor instance")
                # If there are multiple successor instances, return the first one that matches the successor function
                # name and has the correct index
                for successor_instance in successor_instances:
                    if successor_instance.startswith(successor_function_name):
                        if successor_instance.split(":", maxsplit=2)[1] == "merge":
                            return successor_instance
                        if successor_instance.split(":", maxsplit=2)[1].split("_")[-1] == str(self._successor_index):
                            self._successor_index += 1
                            return successor_instance
                raise RuntimeError("Could not find successor instance")
        raise RuntimeError("Could not find current instance")

    def get_routing_decision(self, frame: FrameType) -> dict[str, Any]:
        """
        This is the structure of the routhing decision:

        {
            "run_id": "test_run_id",
            "routing_placement": {
                "test_func": {
                    "provider_region": "aws:region",
                    "identifier": "test_identifier"
                },
                "test_func_1": {
                    "provider_region": "aws:region",
                    "identifier": "test_identifier"
                }
            },
            "current_instance_name": "test_func",
            "instances": [
                {
                    "instance_name": "test_func", "succeeding_instances": ["test_func_1"], "preceding_instances": []
                }
            ]
        }
        """
        # Get the routing decision from the wrapper function
        if "wrapper" in frame.f_locals:
            wrapper = frame.f_locals["wrapper"]
            if hasattr(wrapper, "routing_decision"):
                return wrapper.routing_decision

        raise RuntimeError("Could not get routing decision")

    def get_function__name__from_frame(self, frame: FrameType) -> str:
        # Get the name of the function from the frame
        return frame.f_code.co_name

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

        This method is only invoked if the merge function was
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
        this_frame = inspect.currentframe()
        if not this_frame:
            raise RuntimeError("Could not get current frame")

        previous_frame = this_frame.f_back
        if not previous_frame:
            raise RuntimeError("Could not get previous frame")

        # We need to go back two frames to get the frame of the wrapper function that stores the routing decision
        # and the payload (see more on this in the explanation of the decorator `serverless_function`)
        _, wrapper_frame = self.get_function_and_wrapper_frame(this_frame)

        routing_decision = self.get_routing_decision(wrapper_frame)

        if "current_instance_name" not in routing_decision:
            raise RuntimeError(
                "Could not get current instance name, is this the entry point? Entry point cannot be merge function"
            )

        if "run_id" not in routing_decision:
            raise RuntimeError(
                "Could not get workflow instance id, is this the entry point? Entry point cannot be merge function"
            )

        current_instance_name = routing_decision["current_instance_name"]
        workflow_instance_id = routing_decision["run_id"]

        provider_region = routing_decision["routing_placement"][current_instance_name]["provider_region"].split(":")
        return provider_region[0], provider_region[1], current_instance_name, workflow_instance_id

    def get_routing_decision_from_platform(self) -> dict[str, Any]:
        """
        Get the routing decision from the platform.
        """
        # TODO (#7): Get routing decision from platform
        return {}

    def register_function(
        self,
        function: Callable[..., Any],
        name: str,
        entry_point: bool,
        regions_and_providers: dict,
        environment_variables: list[dict[str, str]],
    ) -> None:
        """
        Register a function as a serverless function.

        Where the function is deployed depends on the routing decision which will be made by the solver.

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

    def serverless_function(
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
            In this case, wrapper unwraps the arguments of func and retrieves the routing decision
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
                if entry_point:
                    wrapper.routing_decision = self.get_routing_decision_from_platform()  # type: ignore
                    # This is the first function to be called, so we need to generate a run id
                    # This run id will be used to identify the workflow instance
                    # For example for the merge function, we need to know which workflow instance to merge
                    wrapper.routing_decision["run_id"] = uuid.uuid4().hex  # type: ignore
                    payload = args[0]
                else:
                    # Get the routing decision from the message received from the predecessor function
                    argument = json.loads(args[0])
                    if "routing_decision" not in argument:
                        raise RuntimeError("Could not get routing decision from message")
                    wrapper.routing_decision = argument["routing_decision"]  # type: ignore
                    payload = argument.get("payload", {})

                # Call the original function with the modified arguments
                return func(payload)

            wrapper.routing_decision = {}  # type: ignore
            wrapper.entry_point = entry_point  # type: ignore
            wrapper.original_function = func  # type: ignore
            self.register_function(func, handler_name, entry_point, regions_and_providers, environment_variables)
            return wrapper

        return _register_handler
