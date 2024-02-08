# Loader
from multi_x_serverless.common.models.endpoints import Endpoints
from multi_x_serverless.common.models.remote_client.remote_client import RemoteClient
from multi_x_serverless.routing.models.dag import DAG
from multi_x_serverless.routing.models.region import Region

# Calculators
from multi_x_serverless.routing.solver.input.components.calculators.carbon_calculator import CarbonCalculator
from multi_x_serverless.routing.solver.input.components.calculators.cost_calculator import CostCalculator
from multi_x_serverless.routing.solver.input.components.calculators.runtime_calculator import RuntimeCalculator
from multi_x_serverless.routing.solver.input.components.loaders.carbon_loader import CarbonLoader
from multi_x_serverless.routing.solver.input.components.loaders.datacenter_loader import DatacenterLoader
from multi_x_serverless.routing.solver.input.components.loaders.performance_loader import PerformanceLoader
from multi_x_serverless.routing.solver.input.components.loaders.region_viability_loader import RegionViabilityLoader
from multi_x_serverless.routing.solver.input.components.loaders.workflow_loader import WorkflowLoader

# Outside library
from multi_x_serverless.routing.workflow_config import WorkflowConfig


class InputManager:
    _region_indexer: Region
    _instance_indexer: DAG

    def __init__(self, config: WorkflowConfig, setup_region_viability: bool = True) -> None:
        super().__init__()
        self._config = config

        # Initialize remote client
        self._data_collector_client: RemoteClient = Endpoints().get_data_collector_client()

        # initialize Loaders
        # Loader Manager
        self._region_viability_loader = RegionViabilityLoader(self._data_collector_client)
        self._datacenter_loader = DatacenterLoader(self._data_collector_client)
        self._performance_loader = PerformanceLoader(self._data_collector_client)
        self._carbon_loader = CarbonLoader(self._data_collector_client)
        self._workflow_loader = WorkflowLoader(self._data_collector_client, self._config.instances)

        # Setup the viability loader and load available regions
        if setup_region_viability:
            self._region_viability_loader.setup()  # Setup the viability loader -> This loads data from the database

        # Calculators
        self._runtime_calculator = RuntimeCalculator(self._performance_loader, self._workflow_loader)
        self._carbon_calculator = CarbonCalculator(
            self._carbon_loader, self._datacenter_loader, self._workflow_loader, self._runtime_calculator
        )
        self._cost_calculator = CostCalculator(self._datacenter_loader, self._workflow_loader, self._runtime_calculator)

    def setup(self, regions_indexer: Region, instance_indexer: DAG) -> None:
        self._region_indexer = regions_indexer
        self._instance_indexer = instance_indexer

        # Get a set of all chosen regions (regions inside the region_indexer)
        requested_regions: set[str] = set(regions_indexer.get_value_indices().keys())

        # Load the workflow loader
        workflow_id = self._config.workflow_id
        if workflow_id is None:
            raise ValueError("Workflow ID is not set in the config")
        self._workflow_loader.setup(workflow_id)

        # Get the set of all favored regions
        favored_regions: set[str] = self._workflow_loader.get_all_favorite_regions()

        # Join the two sets to get the final set of regions
        all_required_regions: set[str] = requested_regions.union(favored_regions)

        # Now setup all appropriate loaders
        self._datacenter_loader.setup(all_required_regions)
        self._performance_loader.setup(all_required_regions)
        self._carbon_loader.setup(all_required_regions)

    def get_execution_cost_carbon_runtime(
        self, instance_index: int, region_index: int, consider_probabilistic_invocations: bool = False
    ) -> list[float]:
        # Convert the instance and region index into the string representation
        instance_name: str = self._instance_indexer.index_to_value(instance_index)
        region_name: str = self._region_indexer.index_to_value(region_index)

        # Calculated the cost, carbon and runtime
        execution_cost = self._cost_calculator.calculate_execution_cost(
            instance_name, region_name, consider_probabilistic_invocations
        )
        execution_carbon = self._carbon_calculator.calculate_execution_carbon(
            instance_name, region_name, consider_probabilistic_invocations
        )
        execution_runtime = self._runtime_calculator.calculate_runtime(
            instance_name, region_name, consider_probabilistic_invocations
        )

        return [execution_cost, execution_carbon, execution_runtime]

    def get_transmission_cost_carbon_runtime(
        self,
        from_instance_index: int,
        to_instance_index: int,
        from_region_index: int,
        to_region_index: int,
        consider_probabilistic_invocations: bool = False,
    ) -> list[float]:
        # Convert the instance and region index into the string representation
        from_instance_name: str = self._instance_indexer.index_to_value(from_instance_index)
        to_instance_name: str = self._instance_indexer.index_to_value(to_instance_index)
        from_region_name: str = self._region_indexer.index_to_value(from_region_index)
        to_region_name: str = self._region_indexer.index_to_value(to_region_index)

        # Calculated the cost, carbon and runtime (latency)
        transmission_cost = self._cost_calculator.calculate_transmission_cost(
            from_instance_name, to_instance_name, from_region_name, to_region_name, consider_probabilistic_invocations
        )
        transmission_carbon = self._carbon_calculator.calculate_transmission_carbon(
            from_instance_name, to_instance_name, from_region_name, to_region_name, consider_probabilistic_invocations
        )
        transmission_runtime = self._runtime_calculator.calculate_latency(
            from_instance_name, to_instance_name, from_region_name, to_region_name, consider_probabilistic_invocations
        )

        return [transmission_cost, transmission_carbon, transmission_runtime]

    def get_all_regions(self) -> list[dict]:
        # Need to convert the regions to a list of dictionaries
        # Where the dict has the following keys: "region", "provider"
        converted_regions: list[dict] = []
        for region in self._region_viability_loader.get_available_regions():
            converted_regions.append({"provider": region[0], "region": region[1]})

        return converted_regions
