from multi_x_serverless.routing.models.instance_indexer import InstanceIndexer
from multi_x_serverless.routing.models.region_indexer import RegionIndexer
from multi_x_serverless.routing.workflow_config import WorkflowConfig


class InputManager:
    _region_indexer: RegionIndexer
    _instance_indexer: InstanceIndexer

    def __init__(self, workflow_config: WorkflowConfig) -> None:
        super().__init__()
        self._workflow_config = workflow_config

    def get_invocation_probability(self, from_instance_index: int, to_instance_index: int) -> float:
        """
        Return the probability of the edge being triggered.
        """
        return 0.0  # TODO: Implement this method

    def get_execution_cost_carbon_runtime(
        self, instance_index: int, region_index: int, probabilistic_case: bool
    ) -> tuple[float, float, float]:
        """
        Return the execution cost, carbon, and runtime of the instance in the given region.
        If probabilistic_case is True, return a RANDOM value from a distribution. (Without replacement)
        If probabilistic_case is False, return the tail value from the distribution.
        """
        return (0.0, 0.0, 0.0)  # TODO: Implement this method

    def get_transmission_cost_carbon_runtime(
        self,
        from_instance_index: int,
        to_instance_index: int,
        from_region_index: int,
        to_region_index: int,
        probabilistic_case: bool,
    ) -> tuple[float, float, float]:
        """
        Return the transmission cost, carbon, and runtime of the transmission between the two instances.
        If probabilistic_case is True, return a RANDOM value from a distribution. (Without replacement)
        If probabilistic_case is False, return the tail value from the distribution.
        """
        return (0.0, 0.0, 0.0)  # TODO: Implement this method

    def get_all_regions(self) -> list[str]:
        return []  # TODO: Implement this method
