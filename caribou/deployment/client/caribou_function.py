from typing import Any, Callable, Optional

from caribou.common.utils import get_function_source


class CaribouFunction:  # pylint: disable=too-many-instance-attributes
    """
    Class that represents a serverless function.
    """

    def __init__(
        self,
        function_callable: Callable[..., Any],
        name: str,
        entry_point: bool,
        regions_and_providers: dict,
        environment_variables: list[dict[str, str]],
        allow_placement_decision_override: bool = False,
    ):
        self.function_callable = function_callable
        self.name = name
        self.entry_point = entry_point
        self.handler = f"app.{function_callable.__name__}"
        self.regions_and_providers = regions_and_providers if len(regions_and_providers) > 0 else None
        self.environment_variables = environment_variables if len(environment_variables) > 0 else None
        self.allow_placement_decision_override = allow_placement_decision_override

        # Will be set when the function is registered with a workflow
        self.wrapped_function: Optional[Callable[..., Any]] = None

        self.validate_function_name()

    def set_wrapped_function(self, wrapped_function: Callable[..., Any]) -> None:
        """
        Set the wrapped version of the function.
        This is called when the function is registered with a workflow.
        """
        self.wrapped_function = wrapped_function

    def validate_function_name(self) -> None:
        """
        Validate the function name.
        """
        if ":" in self.name:
            raise ValueError("Function name cannot contain ':'")

    def is_waiting_for_predecessors(self) -> bool:
        """
        Check whether this function is waiting for its predecessors to finish.
        """
        source_code = get_function_source(self.function_callable)
        return "get_predecessor_data" in source_code
