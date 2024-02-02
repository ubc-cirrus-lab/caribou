# # Inner Components

# # Loader
# from multi_x_serverless.routing.models.indexer import Indexer
# from multi_x_serverless.routing.solver_inputs.components.carbon_input import CarbonInput
# from multi_x_serverless.routing.solver_inputs.components.cost_input import CostInput

# # Data sources
# from multi_x_serverless.routing.solver_inputs.components.data_sources.data_source_manager import DataSourceManager
# from multi_x_serverless.routing.solver_inputs.components.data_sources.source import Source

# # Inputs
# from multi_x_serverless.routing.solver_inputs.components.input import Input
# from multi_x_serverless.routing.solver_inputs.components.loaders.loader_manager import LoaderManager
# from multi_x_serverless.routing.solver_inputs.components.loaders.region_viability_loader import RegionViabilityLoader
# from multi_x_serverless.routing.solver_inputs.components.runtime_input import RuntimeInput

# # Outside library
# from multi_x_serverless.routing.workflow_config import WorkflowConfig


# class InputManager:
#     def __init__(self, config: WorkflowConfig) -> None:
#         super().__init__()
#         self._config = config

#         # initialize components
#         # Loader Manager
#         self._loader_manager = LoaderManager()

#         # Viability loader
#         self._region_viability_loader = RegionViabilityLoader()
#         self._region_viability_loader.setup()  # Setup the viability loader

#         # Data sources
#         self._data_source_manager = DataSourceManager()

#         # Finalized inputs
#         self._carbon_input = CarbonInput()
#         self._cost_input = CostInput()
#         self._runtime_input = RuntimeInput()

#     def setup(self, regions_indexer: Indexer, instance_indexer: Indexer) -> bool:
#         # Regions and instances under consideration
#         regions: list[tuple[str, str]] = list(regions_indexer.get_value_indices().keys())
#         instances: list[str] = list(instance_indexer.get_value_indices().keys())

#         # Workflow loaders use the workfload unique ID from the config
#         workflow_id = self._config.workflow_id
#         if workflow_id is None:
#             return False

#         # Utilize the Loaders to load the data from the database
#         success = self._loader_manager.setup(regions, workflow_id)
#         if not success:
#             return False

#         # Get the retrieved information (From database or cache)
#         all_loaded_informations = self._loader_manager.retrieve_data()

#         # Using those information, we can now setup the data sources
#         instance_configuration = self._config.instances
#         self._data_source_manager.setup(
#             all_loaded_informations, instance_configuration, regions, instances, regions_indexer, instance_indexer
#         )

#         # Now take the loaded data and send it to the data sources, which will be used in the component input managers
#         instances_indicies: list[int] = list(instance_indexer.get_value_indices().values())
#         regions_indicies: list[int] = list(regions_indexer.get_value_indices().values())

#         # First initialize runtime manager, the runtime of this WILL be used in the carbon and cost input managers
#         self._runtime_input.setup(instances_indicies, regions_indicies, self._data_source_manager)

#         self._carbon_input.setup(instances_indicies, regions_indicies, self._data_source_manager, self._runtime_input)
#         self._cost_input.setup(instances_indicies, regions_indicies, self._data_source_manager, self._runtime_input)

#         return True  # At this point we have successfully setup the input manager

#     def get_execution_value(
#         self,
#         desired_calculator: str,
#         instance_index: int,
#         region_index: int,
#         consider_probabilistic_invocations: bool = False,
#     ) -> float:
#         return self._get_input_component_manager(desired_calculator).get_execution_value(
#             instance_index, region_index, consider_probabilistic_invocations
#         )

#     def get_execution_cost_carbon_runtime(
#         self, instance_index: int, region_index: int, consider_probabilistic_invocations: bool = False
#     ) -> list[float]:
#         results = []
#         calculators = ["Cost", "Carbon", "Runtime"]
#         for calculator in calculators:
#             results.append(
#                 self._get_input_component_manager(calculator).get_execution_value(
#                     instance_index, region_index, consider_probabilistic_invocations
#                 )
#             )
#         return results

#     def get_transmission(
#         self,
#         desired_calculator: str,
#         from_instance_index: int,
#         to_instance_index: int,
#         from_region_index: int,
#         to_region_index: int,
#         consider_probabilistic_invocations: bool = False,
#     ) -> float:
#         return self._get_input_component_manager(desired_calculator).get_transmission_value(
#             from_instance_index,
#             to_instance_index,
#             from_region_index,
#             to_region_index,
#             consider_probabilistic_invocations,
#         )

#     def get_transmission_cost_carbon_runtime(
#         self,
#         from_instance_index: int,
#         to_instance_index: int,
#         from_region_index: int,
#         to_region_index: int,
#         consider_probabilistic_invocations: bool = False,
#     ) -> list[float]:
#         results = []
#         calculators = ["Cost", "Carbon", "Runtime"]
#         for calculator in calculators:
#             results.append(
#                 self._get_input_component_manager(calculator).get_transmission_value(
#                     from_instance_index,
#                     to_instance_index,
#                     from_region_index,
#                     to_region_index,
#                     consider_probabilistic_invocations,
#                 )
#             )
#         return results

#     def get_all_regions(self) -> list[dict]:
#         return self._region_viability_loader.retrieve_data()["viable_regions"]

#     def _get_input_component_manager(self, desired_calculator: str) -> Input:
#         return {
#             "Carbon": self._carbon_input,
#             "Cost": self._cost_input,
#             "Runtime": self._runtime_input,
#         }[desired_calculator]
