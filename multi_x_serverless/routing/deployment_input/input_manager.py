import numpy as np

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


class InputManager:
    _region_indexer: RegionIndexer
    _instance_indexer: InstanceIndexer
    _execution_distribution_cache: dict[str, tuple[np.ndarray, np.ndarray, np.ndarray]]
    _transmission_distribution_cache: dict[str, tuple[np.ndarray, np.ndarray, np.ndarray]]
    _invocation_probability_cache: dict[str, float]

    def __init__(self, workflow_config: WorkflowConfig, tail_threshold: int = 95) -> None:
        super().__init__()
        # Set the workflow config
        self._workflow_config: WorkflowConfig = workflow_config

        # Save the tail threshold (Number = percentile for tail runtime/latency)
        # This value MUST be between 50 and 100
        if tail_threshold < 50 or tail_threshold > 100:
            raise ValueError("Tail threshold must be between 50 and 100")
        self._tail_threshold: float = tail_threshold

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
        self._execution_distribution_cache: dict[str, tuple[np.ndarray, np.ndarray, np.ndarray]] = {}
        self._transmission_distribution_cache: dict[str, tuple[np.ndarray, np.ndarray, np.ndarray]] = {}
        self._invocation_probability_cache: dict[str, float] = {}

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

    def get_execution_cost_carbon_runtime_distribution(
        self, instance_index: int, region_index: int
    ) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
        """
        Return the execution cost, carbon, and runtime sample distribution of the instance in the given region.
        """
        # Check if the value is already in the cache
        key = f"{instance_index}_{region_index}"
        if key in self._execution_distribution_cache:
            return self._execution_distribution_cache[key]

        # Convert the instance and region indices to their names
        instance_name = self._instance_indexer.index_to_value(instance_index)
        region_name = self._region_indexer.index_to_value(region_index)

        # If not, calculate the value and store it in the cache
        cost_distribution = self._cost_calculator.calculate_execution_cost_distribution(instance_name, region_name)
        carbon_distribution = self._carbon_calculator.calculate_execution_carbon_distribution(
            instance_name, region_name
        )
        runtime_distribution = self._runtime_calculator.calculate_runtime_distribution(instance_name, region_name)

        execution_cost_carbon_runtime_distribution = (cost_distribution, carbon_distribution, runtime_distribution)
        self._execution_distribution_cache[key] = execution_cost_carbon_runtime_distribution
        return execution_cost_carbon_runtime_distribution

    def get_transmission_cost_carbon_runtime_distribution(
        self,
        from_instance_index: int,
        to_instance_index: int,
        from_region_index: int,
        to_region_index: int,
    ) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
        """
        Return the transmission cost, carbon, and runtime sample distribution of the transmission between the two instances (while taking into account regions).
        """
        # Check if the value is already in the cache
        key = f"{from_instance_index}_{to_instance_index}_{from_region_index}_{to_region_index}"
        if key in self._transmission_distribution_cache:
            return self._transmission_distribution_cache[key]

        # Convert the instance and region indices to their names
        from_instance_name = self._instance_indexer.index_to_value(from_instance_index)
        to_instance_name = self._instance_indexer.index_to_value(to_instance_index)
        from_region_name = self._region_indexer.index_to_value(from_region_index)
        to_region_name = self._region_indexer.index_to_value(to_region_index)

        # If not, calculate the value and store it in the cache
        cost_distribution = self._cost_calculator.calculate_transmission_cost_distribution(
            from_instance_name, to_instance_name, from_region_name, to_region_name
        )
        carbon_distribution = self._carbon_calculator.calculate_transmission_carbon_distribution(
            from_instance_name, to_instance_name, from_region_name, to_region_name
        )
        runtime_distribution = self._runtime_calculator.calculate_latency_distribution(
            from_instance_name, to_instance_name, from_region_name, to_region_name
        )

        transmission_cost_carbon_runtime_distribution = (cost_distribution, carbon_distribution, runtime_distribution)
        self._transmission_distribution_cache[key] = transmission_cost_carbon_runtime_distribution
        return transmission_cost_carbon_runtime_distribution

    def get_execution_cost_carbon_runtime(
        self, instance_index: int, region_index: int, probabilistic_case: bool
    ) -> tuple[float, float, float]:
        """
        Return the execution cost, carbon, and runtime of the instance in the given region.
        If probabilistic_case is True, return a RANDOM value from a distribution. (Without replacement)
        If probabilistic_case is False, return the tail value from the distribution.
        """
        # Get the distribution
        (
            cost_distribution,
            carbon_distribution,
            runtime_distribution,
        ) = self.get_execution_cost_carbon_runtime_distribution(instance_index, region_index)

        return self._consider_probabilistic_invocations(
            cost_distribution, carbon_distribution, runtime_distribution, probabilistic_case
        )

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
        # Get the distribution
        (
            cost_distribution,
            carbon_distribution,
            runtime_distribution,
        ) = self.get_transmission_cost_carbon_runtime_distribution(
            from_instance_index, to_instance_index, from_region_index, to_region_index
        )

        return self._consider_probabilistic_invocations(
            cost_distribution, carbon_distribution, runtime_distribution, probabilistic_case
        )

    def get_all_regions(self) -> list[str]:
        return self._region_viability_loader.get_available_regions()

    def _consider_probabilistic_invocations(
        self,
        cost_distribution: np.ndarray,
        carbon_distribution: np.ndarray,
        runtime_distribution: np.ndarray,
        probabilistic_case: bool,
    ) -> tuple[float, float, float]:
        """
        Return the execution cost, carbon, and runtime base on distributions
        """
        # If probabilistic_case is True, return a RANDOM value from a distribution. (Without replacement)
        if probabilistic_case:
            cost = np.random.choice(cost_distribution)
            carbon = np.random.choice(carbon_distribution)
            runtime = np.random.choice(runtime_distribution)
            return (cost, carbon, runtime)

        # If probabilistic_case is False, return the tail value from the distribution.
        cost = np.percentile(cost_distribution, self._tail_threshold)
        carbon = np.percentile(carbon_distribution, self._tail_threshold)
        runtime = np.percentile(runtime_distribution, self._tail_threshold)

        return (cost, carbon, runtime)
