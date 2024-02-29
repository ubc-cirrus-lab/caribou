from abc import ABC, abstractmethod

from multi_x_serverless.routing.deployment_input.input_manager import InputManager
from multi_x_serverless.routing.models.instance_indexer import InstanceIndexer
from multi_x_serverless.routing.models.region_indexer import RegionIndexer
from multi_x_serverless.routing.workflow_config import WorkflowConfig


class DeploymentMetricsCalculator(ABC):
    def __init__(
        self,
        workflow_config: WorkflowConfig,
        input_manager: InputManager,
        region_indexer: RegionIndexer,
        instance_indexer: InstanceIndexer,
    ):
        self._workflow_config = workflow_config
        self._input_manager: InputManager = input_manager
        self._region_indexer: RegionIndexer = region_indexer
        self._instance_indexer: InstanceIndexer = instance_indexer

    @abstractmethod
    def calculate_deployment_metrics(self, deployment: list[int]) -> dict[str, float]:
        """
        Calculate the deployment metrics for the given deployment.
        Input: deployment: list[int] Where each element is the region index where
        the corresponding instance is deployed.
        Output: dict[str, float] A dictionary with the following keys:
            - average_cost: The average cost of the deployment
            - average_runtime: The average runtime of the deployment
            - average_carbon: The average carbon footprint of the deployment
            - tail_cost: The cost of the deployment with the highest cost
            - tail_runtime: The runtime of the deployment with the highest runtime
            - tail_carbon: The carbon footprint of the deployment with the highest carbon footprint
        """
        raise NotImplementedError
