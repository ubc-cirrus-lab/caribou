import math
import random
import time
from typing import Any, Optional

from caribou.common.constants import TAIL_LATENCY_THRESHOLD
from caribou.common.models.endpoints import Endpoints
from caribou.common.models.remote_client.remote_client import RemoteClient
from caribou.deployment_solver.deployment_input.components.calculators.carbon_calculator import CarbonCalculator
from caribou.deployment_solver.deployment_input.components.calculators.cost_calculator import CostCalculator
from caribou.deployment_solver.deployment_input.components.calculators.runtime_calculator import RuntimeCalculator
from caribou.deployment_solver.deployment_input.components.loaders.carbon_loader import CarbonLoader
from caribou.deployment_solver.deployment_input.components.loaders.datacenter_loader import DatacenterLoader
from caribou.deployment_solver.deployment_input.components.loaders.performance_loader import PerformanceLoader
from caribou.deployment_solver.deployment_input.components.loaders.region_viability_loader import RegionViabilityLoader
from caribou.deployment_solver.deployment_input.components.loaders.workflow_loader import WorkflowLoader
from caribou.deployment_solver.models.instance_indexer import InstanceIndexer
from caribou.deployment_solver.models.region_indexer import RegionIndexer
from caribou.deployment_solver.workflow_config import WorkflowConfig

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
        self._carbon_calculator = CarbonCalculator(self._carbon_loader, self._datacenter_loader, self._workflow_loader)
        self._cost_calculator = CostCalculator(self._datacenter_loader, self._workflow_loader)

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

    def get_all_carbon_data(self) -> dict[str, Any]:
        return self._carbon_loader.get_carbon_data()

    def __getstate__(self):  # type: ignore
        state = self.__dict__.copy()
        state.pop("_data_collector_client", None)
        state.pop("_datacenter_loader", None)
        state.pop("_performance_loader", None)
        state.pop("_runtime_calculator", None)
        state.pop("_cost_calculator", None)
        state["_region_viability_loader"] = self._region_viability_loader.get_available_regions()
        state["_carbon_loader"] = self._carbon_loader.get_carbon_data()
        state["_workflow_loader"] = self._workflow_loader.get_workflow_data()
        state["_carbon_calculator"] = {
            "_energy_factor_of_transmission": self._carbon_calculator._energy_factor_of_transmission,
            "_consider_home_region_for_transmission": self._carbon_calculator._consider_home_region_for_transmission,
        }
        return state

    def __setstate__(self, state):  # type: ignore
        self.__dict__.update(state)
        self._data_collector_client: RemoteClient = Endpoints().get_data_collector_client()  # type: ignore
        self._region_viability_loader = RegionViabilityLoader(self._data_collector_client)
        self._datacenter_loader = DatacenterLoader(self._data_collector_client)
        self._performance_loader = PerformanceLoader(self._data_collector_client)
        self._carbon_loader = CarbonLoader(self._data_collector_client)
        self._workflow_loader = WorkflowLoader(self._data_collector_client, self._workflow_config)
        self._region_viability_loader.setup(state.get("_region_viability_loader"))

        # Setup the calculator
        self._runtime_calculator = RuntimeCalculator(self._performance_loader, self._workflow_loader)
        self._carbon_calculator = CarbonCalculator(self._carbon_loader, self._datacenter_loader, self._workflow_loader)
        self._cost_calculator = CostCalculator(self._datacenter_loader, self._workflow_loader)
        self._carbon_calculator._energy_factor_of_transmission = state.get("_carbon_calculator").get(
            "_energy_factor_of_transmission"
        )
        self._carbon_calculator._consider_home_region_for_transmission = state.get("_carbon_calculator").get(
            "_consider_home_region_for_transmission"
        )
        requested_regions: set[str] = set(self._region_indexer.get_value_indices().keys())

        # Load the workflow loader
        workflow_id = self._workflow_config.workflow_id
        if workflow_id is None:
            raise ValueError("Workflow ID is not set in the config")
        self._workflow_loader.set_workflow_data(state.get("_workflow_loader"))

        # Home region of the workflow should already be in requested regions
        # We should asset this is true
        if self._workflow_loader.get_home_region() not in requested_regions:
            raise ValueError("Home region of the workflow is not in the requested regions! This should NEVER happen!")

        # Now setup all appropriate loaders
        self._datacenter_loader.setup(requested_regions)
        self._performance_loader.setup(requested_regions)
        self._carbon_loader.setup(
            requested_regions,
            carbon_data=state.get("_carbon_loader"),
        )

    ######### New functions #########
    def get_transmission_info(
        self,
        from_instance_index: int,
        from_region_index: int,
        to_instance_index: int,
        to_region_index: int,
        cumulative_runtime: float,
        to_instance_is_sync_node: bool,
    ) -> dict[str, Any]:
        # Convert the instance and region indices to their names
        ## For start hop, from_instance_index and from_region_index will be -1
        from_instance_name: Optional[str] = None
        from_region_name: Optional[str] = None
        if from_instance_index != -1:
            from_instance_name = self._instance_indexer.index_to_value(from_instance_index)
        if from_region_index != -1:
            from_region_name = self._region_indexer.index_to_value(from_region_index)

        ## To instance and region will always have region and instance index
        to_instance_name = self._instance_indexer.index_to_value(to_instance_index)
        to_region_name = self._region_indexer.index_to_value(to_region_index)

        # Get a transmission size and latency sample
        transmission_size, transmission_latency = self._runtime_calculator.calculate_transmission_size_and_latency(
            from_instance_name, from_region_name, to_instance_name, to_region_name, to_instance_is_sync_node
        )

        sns_transmission_size = transmission_size
        sync_info: Optional[dict[str, Any]] = None
        if to_instance_is_sync_node:
            if not from_instance_name:
                raise ValueError("Start hop cannot have a sync node as a successor")

            # If to instance is a sync node, then at the same time,
            # we can retrieve the sync_sizes_gb and sns_only_sizes_gb
            # And then calculate the sync node related information.
            sns_only_size, sync_size, wcu = self._get_upload_sync_size_and_wcu(from_instance_name, to_instance_name)
            sns_transmission_size = sns_only_size
            sync_info = {
                "dynamodb_upload_size": transmission_size,
                "sync_size": sync_size,
                "consumed_dynamodb_write_capacity_units": wcu,
                "sync_upload_auxiliary_info": (cumulative_runtime, transmission_size),
            }

        # If to instance is a sync node, then at the same time,
        # # we can retrieve the sync_sizes_gb and sns_only_sizes_gb
        # And then calculate the sync node related information.
        return {
            "starting_runtime": cumulative_runtime,
            "cumulative_runtime": cumulative_runtime + transmission_latency,
            "sns_data_transfer_size": sns_transmission_size,
            "sync_info": sync_info,
        }

    def _get_upload_sync_size_and_wcu(
        self, from_instance_name: str, to_instance_name: str
    ) -> tuple[float, float, float]:
        # If to instance is a sync node, then at the same time,
        # we can retrieve the sync_sizes_gb and sns_only_sizes_gb
        sns_only_size = self._workflow_loader.get_sns_only_size(from_instance_name, to_instance_name)
        sync_size = self._workflow_loader.get_sync_size(from_instance_name, to_instance_name)

        # We have to get sync_size * 2, as our wrapper does 2 update operations
        dynamodb_write_capacity_units = self._calculate_write_capacity_units(sync_size) * 2

        return sns_only_size, sync_size, dynamodb_write_capacity_units

    def _calculate_write_capacity_units(self, data_size_gb: float) -> float:
        # We can calculate the write capacity units for the data size
        # DynamoDB charges 1 WCU for 1 KB of data written for On-Demand capacity mode
        # https://aws.amazon.com/dynamodb/pricing/on-demand/

        # Convert the data size from GB to KB
        # And then round up to the nearest 1 KB
        data_size_kb = float(math.ceil(data_size_gb * 1024**2))
        write_capacity_units = data_size_kb

        return write_capacity_units

    def get_simulated_transmission_info(
        self,
        from_instance_index: int,
        uninvoked_instance_index: int,
        simulated_sync_predecessor_index: int,
        sync_node_index: int,
        from_region_index: int,
        to_region_index: int,
        cumulative_runtime: float,
    ) -> dict[str, Any]:
        # Convert the instance and region indices to their names
        from_instance_name: str = self._instance_indexer.index_to_value(from_instance_index)
        uninvoked_instance_name: str = self._instance_indexer.index_to_value(uninvoked_instance_index)
        simulated_sync_predecessor_name: str = self._instance_indexer.index_to_value(simulated_sync_predecessor_index)
        sync_node_name: str = self._instance_indexer.index_to_value(sync_node_index)
        from_region_name: str = self._region_indexer.index_to_value(from_region_index)
        to_region_name: str = self._region_indexer.index_to_value(to_region_index)

        # Get a transmission size and latency sample
        # print(f"SIMULATED from_instance_name: {from_instance_name}, uninvoked_instance_name:
        # {uninvoked_instance_name}, simulated_sync_predecessor_name: {simulated_sync_predecessor_name},
        # sync_node_name: {sync_node_name}, from_region_name: {from_region_name},
        # to_region_name: {to_region_name}")
        (
            sns_transmission_size,
            transmission_latency,
        ) = self._runtime_calculator.calculate_simulated_transmission_size_and_latency(
            from_instance_name,
            uninvoked_instance_name,
            simulated_sync_predecessor_name,
            sync_node_name,
            from_region_name,
            to_region_name,
        )

        return {
            "starting_runtime": cumulative_runtime,
            "cumulative_runtime": cumulative_runtime + transmission_latency,
            "sns_data_transfer_size": sns_transmission_size,
        }

    def get_non_execution_info(
        self,
        from_instance_index: int,
        to_instance_index: int,
    ) -> dict[str, list[dict[str, Any]]]:
        # Convert the instance and region indices to their names
        # Start hop will never get non-execution info
        from_instance_name: str = self._instance_indexer.index_to_value(from_instance_index)
        to_instance_name: str = self._instance_indexer.index_to_value(to_instance_index)

        non_execution_info_list: list[dict[str, Any]] = []
        for sync_to_from_instance, sync_size in self._workflow_loader.get_non_execution_information(
            from_instance_name, to_instance_name
        ).items():
            parsed_sync_to_from_instance = sync_to_from_instance.split(">")
            sync_predecessor_instance = parsed_sync_to_from_instance[0]
            sync_node_instance = parsed_sync_to_from_instance[1]

            non_execution_info_list.append(
                {
                    "predecessor_instance_id": self._instance_indexer.value_to_index(sync_predecessor_instance),
                    "sync_node_instance_id": self._instance_indexer.value_to_index(sync_node_instance),
                    "sync_size": sync_size,
                    "consumed_dynamodb_write_capacity_units": self._calculate_write_capacity_units(sync_size)
                    * 2,  # We have to get sync_size * 2, as our wrapper does 2 update operations
                }
            )

        return {"non_execution_info": non_execution_info_list}

    def get_node_runtimes_and_data_transfer(
        self, instance_index: int, region_index: int, previous_cumulative_runtime: float
    ) -> tuple[dict[str, Any], float]:
        # Convert the instance and region indices to their names
        instance_name: str = self._instance_indexer.index_to_value(instance_index)
        region_name: str = self._region_indexer.index_to_value(region_index)

        # Get the node runtimes and data transfer information
        node_runtime_data_transfer_data = self._runtime_calculator.calculate_node_runtimes_and_data_transfer(
            instance_name, region_name, previous_cumulative_runtime, self._instance_indexer
        )

        return node_runtime_data_transfer_data

    def calculate_cost_and_carbon_of_instance(
        self,
        runtime: float,
        instance_index: int,
        region_index: int,
        data_input_sizes: dict[int, float],
        data_output_sizes: dict[int, float],
        sns_data_call_and_output_sizes: dict[int, list[float]],
        data_transfer_during_execution: float,
        dynamodb_read_capacity: float,
        dynamodb_write_capacity: float,
        is_invoked: bool,
    ) -> dict[str, float]:
        # Convert the instance and region indices to their names
        instance_name: str = self._instance_indexer.index_to_value(instance_index)
        region_name: str = self._region_indexer.index_to_value(region_index)

        # print("calculate_cost_and_carbon_of_instance")
        # print(f"runtime: {runtime}")
        # print(f"region_index: {region_index}")
        # print(f"data_input_sizes: {data_input_sizes}")
        # print(f"data_output_sizes: {data_output_sizes}")
        # print(f"sns_data_output_sizes: {sns_data_output_sizes}")
        # print(f"data_transfer_during_execution: {data_transfer_during_execution}")
        # print(f"dynamodb_read_capacity: {dynamodb_read_capacity}")
        # print(f"dynamodb_write_capacity: {dynamodb_write_capacity}\n")
        data_output_sizes_str_dict = self._get_converted_region_name_dict(data_output_sizes)
        execution_carbon, transmission_carbon = self._carbon_calculator.calculate_instance_carbon(
            runtime,
            instance_name,
            region_name,
            self._get_converted_region_name_dict(data_input_sizes),
            data_output_sizes_str_dict,
            data_transfer_during_execution,
            is_invoked,
        )
        return {
            "cost": self._cost_calculator.calculate_instance_cost(
                runtime,
                instance_name,
                region_name,
                data_output_sizes_str_dict,
                self._get_converted_region_name_dict(sns_data_call_and_output_sizes),
                dynamodb_read_capacity,
                dynamodb_write_capacity,
                is_invoked,
            ),
            "execution_carbon": execution_carbon,
            "transmission_carbon": transmission_carbon,
        }

    def _get_converted_region_name_dict(self, input_region_index_dict: dict[int, Any]) -> dict[Optional[str], Any]:
        return {
            self._region_indexer.index_to_value(region_index) if region_index != -1 else None: value
            for region_index, value in input_region_index_dict.items()
        }

    def calculate_dynamodb_capacity_unit_of_sync_edges(
        self, sync_edge_upload_edges_auxiliary_data: list[tuple[float, float]]
    ) -> dict[str, float]:
        # Each entry of the sync_edge_upload_edges_auxiliary_data is a tuple
        # Where the first element is when a node reaches the invoke_call
        # Where the second element is the size of the sync uploads
        # The third element is the latency of the entire tranmsission (May be used)
        # We need to first sort the list by the first element (shortest time first)
        # Then we calculate the WRU for each entry with cumulative data sizes
        write_capacity_units = 0.0
        cumulative_data_size = 0.0

        # Sort the list by the first element
        sync_edge_upload_edges_auxiliary_data.sort(key=lambda x: x[0])
        for entry in sync_edge_upload_edges_auxiliary_data:
            # The entry is a tuple of (cumulative_runtime, sync_size, transmission_latency)
            _, sync_size = entry
            cumulative_data_size += sync_size
            write_capacity_units += self._calculate_write_capacity_units(cumulative_data_size)

        return {
            "read_capacity_units": self._calculate_write_capacity_units(cumulative_data_size),
            "write_capacity_units": write_capacity_units,
        }
