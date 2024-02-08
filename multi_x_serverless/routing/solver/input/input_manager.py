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
        self._carbon_calculator = CarbonCalculator(self._carbon_loader, self._datacenter_loader, self._workflow_loader, self._runtime_calculator)
        self._cost_calculator = CostCalculator(self._datacenter_loader, self._workflow_loader, self._runtime_calculator)

    def setup(self, regions_indexer: Region, instance_indexer: DAG) -> bool:
        # # Need to convert it back
        # for region in converted_regions:
        #     (region["provider"], region["region"])

        # # Regions and instances under consideration
        # regions: list[tuple[str, str]] = list(regions_indexer.get_value_indices().keys())
        # instances: list[str] = list(instance_indexer.get_value_indices().keys())

        # # Workflow loaders use the workfload unique ID from the config
        # workflow_id = self._config.workflow_id
        # if workflow_id is None:
        #     return False

        # # Utilize the Loaders to load the data from the database
        # success = self._loader_manager.setup(regions, workflow_id)
        # if not success:
        #     return False

        # # Get the retrieved information (From database or cache)
        # all_loaded_informations = self._loader_manager.retrieve_data()

        # # Using those information, we can now setup the data sources
        # instance_configuration = self._config.instances
        # self._data_source_manager.setup(
        #     all_loaded_informations, instance_configuration, regions, instances, regions_indexer, instance_indexer
        # )

        # # Now take the loaded data and send it to the data sources, which will be used in the component input managers
        # instances_indicies: list[int] = list(instance_indexer.get_value_indices().values())
        # regions_indicies: list[int] = list(regions_indexer.get_value_indices().values())

        # # First initialize runtime manager, the runtime of this WILL be used in the carbon and cost input managers
        # self._runtime_input.setup(instances_indicies, regions_indicies, self._data_source_manager)

        # self._carbon_input.setup(instances_indicies, regions_indicies, self._data_source_manager, self._runtime_input)
        # self._cost_input.setup(instances_indicies, regions_indicies, self._data_source_manager, self._runtime_input)

        return True  # At this point we have successfully setup the input manager

    def get_execution_cost_carbon_runtime(
        self, instance_index: int, region_index: int, consider_probabilistic_invocations: bool = False
    ) -> list[float]:
        # results = []
        # calculators = ["Cost", "Carbon", "Runtime"]
        # for calculator in calculators:
        #     results.append(
        #         self._get_input_component_manager(calculator).get_execution_value(
        #             instance_index, region_index, consider_probabilistic_invocations
        #         )
        #     )
        # return results

        return [0.0, 0.0, 0.0]

    def get_transmission_cost_carbon_runtime(
        self,
        from_instance_index: int,
        to_instance_index: int,
        from_region_index: int,
        to_region_index: int,
        consider_probabilistic_invocations: bool = False,
    ) -> list[float]:
        # results = []
        # calculators = ["Cost", "Carbon", "Runtime"]
        # for calculator in calculators:
        #     results.append(
        #         self._get_input_component_manager(calculator).get_transmission_value(
        #             from_instance_index,
        #             to_instance_index,
        #             from_region_index,
        #             to_region_index,
        #             consider_probabilistic_invocations,
        #         )
        #     )
        return [0.0, 0.0, 0.0]

    def get_all_regions(self) -> list[dict]:
        # Need to convert the regions to a list of dictionaries
        # Where the dict has the following keys: "region", "provider"
        converted_regions: list[dict] = []
        for region in self._region_viability_loader.get_available_regions():
            converted_regions.append({"provider": region[0], "region": region[1]})

        return converted_regions
