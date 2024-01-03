from __future__ import annotations

import inspect
import json
import re
from types import FrameType
from typing import Any, Callable, Optional

from multi_x_serverless.deployment.client.clients import AWSClient
from multi_x_serverless.deployment.client.enums import Endpoint


class MultiXServerlessFunction:  # pylint: disable=too-many-instance-attributes
    """
    Class that represents a serverless function.
    """

    def __init__(
        self,
        function: Callable[..., Any],
        name: str,
        entry_point: bool,
        regions_and_providers: dict,
        providers: list[dict],
    ):
        self.function = function
        self.name = name
        self.entry_point = entry_point
        self.handler = function.__name__
        self.regions_and_providers = regions_and_providers if len(regions_and_providers) > 0 else None
        self.providers = providers if len(providers) > 0 else None

    def get_successors(self, workflow: MultiXServerlessWorkflow) -> list[MultiXServerlessFunction]:
        """
        Get the functions that are called by this function.
        """
        source_code = inspect.getsource(self.function)
        function_calls = re.findall(r"invoke_serverless_function\((.*?)\)", source_code)
        successors: list[MultiXServerlessFunction] = []
        for call in function_calls:
            if "," not in call:
                raise RuntimeError(
                    f"""Could not parse function call ({call}) in function
                    ({self.function}), did you provide the payload?"""
                )
            function_name = call.strip().split(",")[0].strip('"')
            successor = next((func for func in workflow.functions if func.function.__name__ == function_name), None)
            if successor:
                successors.append(successor)
        return successors

    def is_waiting_for_predecessors(self) -> bool:
        """
        Check whether this function is waiting for its predecessors to finish.
        """
        source_code = inspect.getsource(self.function)
        return "get_predecessor_data" in source_code


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
        self.functions: list[MultiXServerlessFunction] = []

    def invoke_serverless_function(
        self, function: Callable[..., Any], payload: Optional[dict | Any] = None  # pylint: disable=unused-argument
    ) -> None:
        """
        Invoke a serverless function which is part of this workflow.
        """
        if not payload:
            payload = {}
        # TODO (#11): Implement conditional invocation
        # If the function from which this function is called is the entry point obtain current routing decision
        # If not, the routing decision was stored in the message received from the predecessor function
        # Post message to SNS -> return
        # Do not wait for response

        frame = inspect.currentframe()
        if not frame:
            raise RuntimeError("Could not get current frame")

        # We need to go back two frames to get the frame of the wrapper function that stores the routing decision
        # and the payload (see more on this in the explanation of the decorator `serverless_function`)
        frame = frame.f_back

        if not frame:
            raise RuntimeError("Could not get previous frame")

        frame = frame.f_back

        if not frame:
            raise RuntimeError("Could not get previous frame")

        routing_decision = self.get_routing_decision(frame)

        # Wrap the payload and add the routing decision
        payload_wrapper = {"payload": payload}
        payload_wrapper["routing_decision"] = routing_decision
        json_payload = json.dumps(payload_wrapper)

        # TODO (#7): The routing decision has to be retrieved from contextual information
        # (where in the workflow are we?)
        # provider, region, arn = routing_decision["next_endpoint"].split(":")
        provider, region, arn = "aws", "us-west-2", "test"
        if provider == Endpoint.AWS.value:
            self.invoke_function_through_sns(json_payload, region, arn)

    def get_routing_decision(self, frame: FrameType) -> dict[str, Any]:
        if "wrapper" in frame.f_locals:
            wrapper = frame.f_locals["wrapper"]
            if hasattr(wrapper, "routing_decision"):
                return wrapper.routing_decision

        raise RuntimeError("Could not get routing decision")

    def invoke_function_through_sns(self, message: str, region: str, arn: str) -> None:
        aws_client = AWSClient(region)
        try:
            aws_client.send_message_to_sns(arn, message)
        except Exception as e:
            raise RuntimeError("Could not invoke function through SNS") from e

    def get_predecessor_data(self) -> list[dict[str, Any]]:
        """
        Get the data returned by the predecessor functions.
        """
        # Check if all predecessor functions have returned
        # If not, abort this function call, another function will eventually be called
        # TODO (#10): Check if all predecessor functions have returned
        return []

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
        providers: list,
    ) -> None:
        """
        Register a function as a serverless function.

        Where the function is deployed depends on the routing decision which will be made by the solver.

        At this point we only need to register the function with the wrapper, the actual deployment will be done
        later by the deployment manager.
        """
        wrapper = MultiXServerlessFunction(function, name, entry_point, regions_and_providers, providers)
        self.functions.append(wrapper)

    # TODO (#22): Add function specific environment variables
    def serverless_function(
        self,
        name: Optional[str] = None,
        entry_point: bool = False,
        regions_and_providers: Optional[dict] = None,
        providers: Optional[list[dict]] = None,
    ) -> Callable[..., Any]:
        """
        Decorator to register a function as a Lambda function.

        :param name: The name of the Lambda function. Defaults to the name of the function being decorated.
        :param entry_point: Whether this function is the entry point for the workflow.

        The following is mildly complicated, but it is necessary to make the decorator work.

        The three layers of functions are used to create a decorator with arguments.

        Outermost function (serverless_function):
            This is the decorator factory. It takes in arguments for the decorator and returns the actual decorator function.
            The arguments passed to this function are used to configure the behavior of the decorator.
            In this case, name, entry_point, regions_and_providers, and providers are used to configure the Lambda function.

        Middle function (_register_handler):
            This is the actual decorator function. It takes in a single argument, which is the function to be decorated.
            It returns a new function that wraps the original function and modifies its behavior.
            In this case, _register_handler takes in func, which is the function to be decorated, and returns wrapper,
            which is a new function that wraps func.
            The middle function is responsible for creating the wrapper function and returning it as well as registering
            the function with the workflow.

        Innermost function (wrapper):
            This is the wrapper function that modifies the behavior of the original function.
            It takes the same arguments as the original function and can modify these arguments before calling the original function.
            It can also modify the return value of the original function.
            In this case, wrapper unwraps the arguments of func and retrieves the routing decision and calls func with the modified unwrapped payload.
        """
        if regions_and_providers is None:
            regions_and_providers = {}

        if providers is None:
            providers = []

        def _register_handler(func: Callable[..., Any]) -> Callable[..., Any]:
            handler_name = name if name is not None else func.__name__

            def wrapper(*args, **kwargs):  # type: ignore # pylint: disable=unused-argument
                # Modify args and kwargs here as needed
                if entry_point:
                    wrapper.routing_decision = self.get_routing_decision_from_platform()  # type: ignore
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
            self.register_function(func, handler_name, entry_point, regions_and_providers, providers)
            return wrapper

        return _register_handler
