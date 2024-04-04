import random
import time
from typing import Optional

from multi_x_serverless.common.constants import TAIL_LATENCY_THRESHOLD
from multi_x_serverless.common.models.endpoints import Endpoints
from multi_x_serverless.common.models.remote_client.remote_client import RemoteClient
from multi_x_serverless.routing.deployment_input.components.calculators.carbon_calculator import CarbonCalculator
from multi_x_serverless.routing.deployment_input.components.calculators.cost_calculator import CostCalculator
from multi_x_serverless.routing.deployment_input.components.calculators.runtime_calculator import RuntimeCalculator
from multi_x_serverless.routing.deployment_input.components.loaders.carbon_loader import CarbonLoader
from multi_x_serverless.routing.deployment_input.components.loaders.datacenter_loader import DatacenterLoader
from multi_x_serverless.routing.deployment_input.components.loaders.performance_loader import PerformanceLoader
from multi_x_serverless.routing.deployment_input.components.loaders.region_viability_loader import RegionViabilityLoader
from multi_x_serverless.routing.deployment_input.components.loaders.workflow_loader import WorkflowLoader
from multi_x_serverless.routing.models.instance_indexer import InstanceIndexer
from multi_x_serverless.routing.models.region_indexer import RegionIndexer
from multi_x_serverless.routing.workflow_config import WorkflowConfig

random.seed(time.time())


class InputManager:  # pylint: disable=too-many-instance-attributes
    _region_indexer: RegionIndexer
    _instance_indexer: InstanceIndexer
    _execution_latency_distribution_cache: dict[str, list[float]]
    _invocation_probability_cache: dict[str, float]

    def __init__(self, workflow_config: WorkflowConfig, tail_latency_threshold: int = TAIL_LATENCY_THRESHOLD) -> None:
        super().__init__()
        # Set the workflow config
        self._workflow_config: WorkflowConfig = workflow_config

        # Save the tail threshold (Number = percentile for tail runtime/latency)
        # This value MUST be between 50 and 100
        if tail_latency_threshold < 50 or tail_latency_threshold > 100:
            raise ValueError("Tail threshold must be between 50 and 100")
        self._tail_latency_threshold: float = tail_latency_threshold

        # Initialize remote client
        self._data_collector_client: RemoteClient = Endpoints().get_data_collector_client()

        # Initialize loaders
        self._region_viability_loader = RegionViabilityLoader(self._data_collector_client)
        self._datacenter_loader = DatacenterLoader(self._data_collector_client)
        self._performance_loader = PerformanceLoader(self._data_collector_client)
        self._carbon_loader = CarbonLoader(self._data_collector_client)
        self._workflow_loader = WorkflowLoader(self._data_collector_client, workflow_config)

        # Setup the viability loader and load the availability regions
        self._region_viability_loader.setup()  # Setup the viability loader -> This loads data from the database

        # Setup the calculator
        self._runtime_calculator = RuntimeCalculator(self._performance_loader, self._workflow_loader)
        self._carbon_calculator = CarbonCalculator(
            self._carbon_loader, self._datacenter_loader, self._workflow_loader, self._runtime_calculator
        )
        self._cost_calculator = CostCalculator(self._datacenter_loader, self._workflow_loader, self._runtime_calculator)

    def setup(self, regions_indexer: RegionIndexer, instance_indexer: InstanceIndexer) -> None:
        self._region_indexer = regions_indexer
        self._instance_indexer = instance_indexer

        # Get a set of all chosen regions (regions inside the region_indexer)
        requested_regions: set[str] = set(regions_indexer.get_value_indices().keys())

        # Load the workflow loader
        workflow_id = self._workflow_config.workflow_id
        if workflow_id is None:
            raise ValueError("Workflow ID is not set in the config")
        self._workflow_loader.setup(workflow_id)

        # Home region of the workflow should already be in requested regions
        # We should asset this is true
        if self._workflow_loader.get_home_region() not in requested_regions:
            raise ValueError("Home region of the workflow is not in the requested regions! This should NEVER happen!")

        # Now setup all appropriate loaders
        self._datacenter_loader.setup(requested_regions)
        self._performance_loader.setup(requested_regions)
        self._carbon_loader.setup(requested_regions)

        # Clear cache
        self._invocation_probability_cache: dict[str, float] = {}
        self._execution_latency_distribution_cache: dict[str, list[float]] = {}

    def get_execution_cost_carbon_latency(self, instance_index: int, region_index: int) -> tuple[float, float, float]:
        # Convert the instance and region indices to their names
        instance_name = self._instance_indexer.index_to_value(instance_index)
        region_name = self._region_indexer.index_to_value(region_index)

        # For this we would need the excecution latency.
        ## First check if the value is already in the cache
        key = f"{instance_index}_{region_index}"
        if key in self._execution_latency_distribution_cache:
            execution_latency_distribution = self._execution_latency_distribution_cache[key]
        else:
            # If not, calculate the value and store it in the cache
            execution_latency_distribution = self._runtime_calculator.calculate_runtime_distribution(
                instance_name, region_name
            )
            self._execution_latency_distribution_cache[key] = execution_latency_distribution

        # Now we can get a random sample from the distribution
        execution_latency = execution_latency_distribution[
            int(random.random() * (len(execution_latency_distribution) - 1))
        ]

        # Now we can calculate the cost and carbon
        cost = self._cost_calculator.calculate_execution_cost(instance_name, region_name, execution_latency)
        carbon = self._carbon_calculator.calculate_execution_carbon(instance_name, region_name, execution_latency)

        return (cost, carbon, execution_latency)

    def get_transmission_cost_carbon_latency(
        self,
        from_instance_index: int,
        to_instance_index: int,
        from_region_index: int,
        to_region_index: int,
    ) -> tuple[float, float, float]:
        # Convert the instance and region indices to their names
        from_instance_name: Optional[str] = None
        if from_instance_index != -1:
            from_instance_name = self._instance_indexer.index_to_value(from_instance_index)
        to_instance_name = self._instance_indexer.index_to_value(to_instance_index)
        from_region_name = self._region_indexer.index_to_value(from_region_index)
        to_region_name = self._region_indexer.index_to_value(to_region_index)

        # Get the transmission size distribution
        transmission_size_distribution: list[float] = self._runtime_calculator.get_transmission_size_distribution(
            from_instance_name, to_instance_name, from_region_name, to_region_name
        )

        # Pick a transmission size or default to None
        transmission_size: Optional[float] = None
        if len(transmission_size_distribution) > 0:
            transmission_size = transmission_size_distribution[
                int(random.random() * (len(transmission_size_distribution) - 1))
            ]

        # Get the transmission latency distribution
        transmission_latency_distribution: list[float] = self._runtime_calculator.get_transmission_latency_distribution(
            from_instance_name, to_instance_name, from_region_name, to_region_name, transmission_size
        )

        # Now we can get a random sample from the distribution
        transmission_latency: float = transmission_latency_distribution[
            int(random.random() * (len(transmission_latency_distribution) - 1))
        ]

        if (
            transmission_size is None
        ):  # At this point we can assume that the transmission size is 0 for any missing data
            transmission_size = 0.0

        # Now we can calculate the cost and carbon
        # If start hop, no transmission cost
        cost = 0.0
        if from_instance_name is not None:  # Start hop should not incur transmission cost
            cost = self._cost_calculator.calculate_transmission_cost(
                from_region_name,
                to_region_name,
                transmission_size,
            )

        carbon = self._carbon_calculator.calculate_transmission_carbon(
            from_region_name, to_region_name, transmission_size
        )

        return (cost, carbon, transmission_latency)

    def alter_carbon_setting(self, carbon_setting: Optional[str]) -> None:
        """
        Input should either be 'None' or a string from '0' to '23' indicating the hour of the day.
        """
        self._carbon_calculator.alter_carbon_setting(carbon_setting)
        self._runtime_calculator.reset_cache()

        # Clear the cache
        self._execution_latency_distribution_cache = {}
        self._invocation_probability_cache = {}

    def get_invocation_probability(self, from_instance_index: int, to_instance_index: int) -> float:
        """
        Return the probability of the edge being triggered.
        """
        # Check if the value is already in the cache
        key = f"{from_instance_index}_{to_instance_index}"
        if key in self._invocation_probability_cache:
            return self._invocation_probability_cache[key]

        # Convert the instance indices to their names
        from_instance_name = self._instance_indexer.index_to_value(from_instance_index)
        to_instance_name = self._instance_indexer.index_to_value(to_instance_index)

        # If not, retrieve the value from the workflow loader
        invocation_probability = self._workflow_loader.get_invocation_probability(from_instance_name, to_instance_name)
        self._invocation_probability_cache[key] = invocation_probability
        return invocation_probability

    def get_all_regions(self) -> list[str]:
        return self._region_viability_loader.get_available_regions()
