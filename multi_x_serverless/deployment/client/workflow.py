from __future__ import annotations

import inspect
import json
import re
from typing import Any, Callable, Optional


class MultiXServerlessFunction:  # pylint: disable=too-many-instance-attributes
    """
    Class that represents a serverless function.
    """

    def __init__(
        self, function: Callable[..., Any], name: str, entry_point: bool, timeout: int, memory: int, region_group: str
    ):
        self.function = function
        self.name = name
        self.entry_point = entry_point
        self.handler = function.__name__
        self.timeout = timeout
        self.memory = memory
        self.region_group = region_group

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
                    f"Could not parse function call ({call}) in function ({self.function}), did you provide the payload?"
                )
            function_name = call.strip().split(",")[0]
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
        self, function: Callable[..., Any], payload: Optional[dict] = None  # pylint: disable=unused-argument
    ) -> Any:
        """
        Invoke a serverless function which is part of this workflow.
        """
        # If the function from which this function is called is the entry point obtain current routing decision
        # If not, the routing decision was stored in the message received from the predecessor function
        # Post message to SNS -> return
        # Do not wait for response
        frame = inspect.currentframe()
        if not frame:
            raise RuntimeError("Could not get current frame")
        frame = frame.f_back

        routing_decision = None

        if not frame:
            raise RuntimeError("Could not get previous frame")

        if frame.f_locals.get("entry_point"):
            routing_decision = frame.f_locals.get("routing_decision")
        else:
            args, _, _, _ = inspect.getargvalues(frame)
            routing_decision = json.loads(args[0])["routing_decision"]

        print(routing_decision)
        # TODO: Post message to messaging services
        return "Some response"

    def get_predecessor_data(self) -> list[dict[str, Any]]:
        """
        Get the data returned by the predecessor functions.
        """
        # Check if all predecessor functions have returned
        # If not, abort this function call, another function will eventually be called
        # TODO: Check if all predecessor functions have returned
        return []

    def register_function(
        self, function: Callable[..., Any], name: str, entry_point: bool, timeout: int, memory: int, region_group: str
    ) -> None:
        """
        Register a function as a serverless function.

        Where the function is deployed depends on the routing decision which will be made by the solver.

        At this point we only need to register the function with the wrapper, the actual deployment will be done
        later by the deployment manager.
        """
        wrapper = MultiXServerlessFunction(function, name, entry_point, timeout, memory, region_group)
        self.functions.append(wrapper)

    def serverless_function(
        self,
        name: Optional[str] = None,
        entry_point: bool = False,
        timeout: int = -1,
        memory: int = 128,
        region_group: str = "default",
    ) -> Callable[..., Any]:
        """
        Decorator to register a function as a Lambda function.

        :param name: The name of the Lambda function. Defaults to the name of the function being decorated.
        :param entry_point: Whether this function is the entry point for the workflow.
        """

        def _register_handler(func: Callable[..., Any]) -> Callable[..., Any]:
            handler_name = name if name is not None else func.__name__

            func.entry_point = entry_point  # type: ignore

            if entry_point:
                func.routing_decision = None  # type: ignore
                # TODO: Get routing decision from platform

            self.register_function(func, handler_name, entry_point, timeout, memory, region_group)
            return func

        return _register_handler
