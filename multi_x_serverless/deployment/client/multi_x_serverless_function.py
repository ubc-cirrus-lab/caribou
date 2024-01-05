import inspect
from typing import Any, Callable


class MultiXServerlessFunction:
    """
    Class that represents a serverless function.
    """

    def __init__(
        self,
        function_callable: Callable[..., Any],
        name: str,
        entry_point: bool,
        regions_and_providers: dict,
        providers: list[dict],
    ):
        self.function_callable = function_callable
        self.name = name
        self.entry_point = entry_point
        self.handler = function_callable.__name__
        self.regions_and_providers = regions_and_providers if len(regions_and_providers) > 0 else None
        self.providers = providers if len(providers) > 0 else None

    def is_waiting_for_predecessors(self) -> bool:
        """
        Check whether this function is waiting for its predecessors to finish.
        """
        source_code = inspect.getsource(self.function_callable)
        return "get_predecessor_data" in source_code
