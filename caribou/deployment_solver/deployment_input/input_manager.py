import multiprocessing
import pdb
import random
import time
from typing import Optional
from multiprocessing import Queue, Process, Value, Manager
from collections import ChainMap

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


def _run_input_manager(
        workflow_config: WorkflowConfig,
        tail_latency_threshold,
        region_indexer: RegionIndexer,
        instance_indexer: InstanceIndexer,
        input_queue: Queue,
        output_queue: Queue,
        pv_queue: Queue,
):
    input_manager: InputManager = InputManager(workflow_config, tail_latency_threshold)
    input_manager.setup(region_indexer, instance_indexer)
    while True:
        try:
            task = input_queue.get(timeout=3)
        except:
            continue
        if task['type'] == 'transmission':
            key = (
                task['input']['from_instance_index'],
                task['input']['to_instance_index'],
                task['input']['from_region_index'],
                task['input']['to_region_index'],
            )
            result = input_manager.get_transmission_cost_carbon_latency(**task['input'])
        else:
            key = (
                task['input']['instance_index'],
                task['input']['region_index']
            )
            result = input_manager.get_execution_cost_carbon_latency(**task['input'])
        output_queue.put((key, result))

        pv_queue.put(input_manager.get_cache())
        # print('Put in Cache')
        cache_sync = pv_queue.get()
#         print('Read from Cache')
        input_manager.update_cache(cache_sync)


class ConcurrentInputManager:
    def __init__(
            self,
            workflow_config: WorkflowConfig,
            region_indexer: RegionIndexer,
            instance_indexer: InstanceIndexer,
            tail_latency_threshold: int = TAIL_LATENCY_THRESHOLD,
            n_workers: int = 4,
    ):
        self.n_workers = n_workers
        self._workflow_config = workflow_config
        self._tail_latency_threshold = tail_latency_threshold
        self._region_indexer = region_indexer
        self._instance_indexer = instance_indexer
        self._setup()

    def run_input_manager(self):
        for i in range(self.n_workers):
            p = Process(
                target=_run_input_manager,
                args=(
                    self._workflow_config,
                    self._tail_latency_threshold,
                    self._region_indexer,
                    self._instance_indexer,
                    self._input_queue,
                    self._output_queue,
                    self._pv_queue_list[i],
                )
            )
            p.start()
            self._pool.append(p)

    def _setup(self):
        self._pool = []
        self._task_manager = Manager()
        self._input_queue = self._task_manager.Queue()
        self._output_queue = self._task_manager.Queue()
        self._pv_queue_list = [self._task_manager.Queue() for _ in range(self.n_workers)]

    def _sync_cache(self):
        pdb.set_trace()
        caches_list = []
        for i in range(self.n_workers):
            caches_list.append(self._pv_queue_list[i].get())

        cache = {
            'execution_latency_distribution_cache': dict(
                ChainMap(*[
                    c['execution_latency_distribution_cache'] for c in caches_list
                ])
            ),
            'invocation_probability_cache': dict(
                ChainMap(*[
                    c['invocation_probability_cache'] for c in caches_list
                ])
            ),
            'workflow_loader_cache': WorkflowLoader.sync_caches(
                [c['workflow_loader_cache'] for c in caches_list]
            ),
            'runtime_calculator_cache': RuntimeCalculator.sync_caches(
                [c['runtime_calculator_cache'] for c in caches_list]
            ),
            'carbon_calculator_cache': CarbonCalculator.sync_caches(
                [c['carbon_calculator_cache'] for c in caches_list]
            ),
            'cost_calculator_cache': CostCalculator.sync_caches(
                [c['cost_calculator_cache'] for c in caches_list]
            ),
        }

        for i in range(self.n_workers):
            self._pv_queue_list[i].put(cache)

    def get_transmission_execution_cost_carbon_latency(
            self,
            transmission_input: list[dict],
            execution_input: list[dict]
    ):
        tasks = [
            {'type': 'transmission', 'input': t_input} for t_input in transmission_input
        ] + [
            {'type': 'execution', 'input': e_input} for e_input in execution_input
        ]

        for task in tasks:
            self._input_queue.put(task)

        tasks_result = []
        for _ in range(len(tasks)):
            tasks_result.append(self._output_queue.get())

        self._sync_cache()

        transmission_result = {}
        execution_result = {}
        for i, task in enumerate(tasks):
            if task['type'] == 'transmission':
                key = (
                    task['input']['from_instance_index'],
                    task['input']['to_instance_index'],
                    task['input']['from_region_index'],
                    task['input']['to_region_index'],
                )
                transmission_result[key] = tasks_result[i]
            else:
                key = (
                    task['input']['instance_index'],
                    task['input']['region_index']
                )
                execution_result[key] = tasks_result[i]
        return transmission_result, execution_result

    def __del__(self):
        for p in self._pool:
            p.terminate()


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

    def get_dict(self):
        return {
            'workflow_config': self._workflow_config.get_dict(),
            'region_indexer': self._region_indexer.get_regions(),
            'instance_indexer': self._instance_indexer.get_nodes(),
            'tail_latency_threshold': int(self._tail_latency_threshold),
        }

    def get_cache(self):
        cache = {
            'execution_latency_distribution_cache': self._execution_latency_distribution_cache,
            'invocation_probability_cache': self._invocation_probability_cache,
            'workflow_loader_cache': self._workflow_loader.get_cache(),
            'runtime_calculator_cache': self._runtime_calculator.get_cache(),
            'carbon_calculator_cache': self._carbon_calculator.get_cache(),
            'cost_calculator_cache': self._cost_calculator.get_cache()
        }
        return cache

    def update_cache(self, cache: dict):
        self._execution_latency_distribution_cache.update(cache['execution_latency_distribution_cache'])
        self._invocation_probability_cache.update(cache['invocation_probability_cache'])
        self._workflow_loader.update_cache(cache['workflow_loader_cache'])
        self._runtime_calculator.update_cache(cache['runtime_calculator_cache'])
        self._carbon_calculator.update_cache(cache['carbon_calculator_cache'])
        self._cost_calculator.update_cache(cache['cost_calculator_cache'])

    @staticmethod
    def from_dict(input_manager_dict: dict):
        workflow_config = WorkflowConfig(input_manager_dict.get('workflow_config'))
        region_indexer = RegionIndexer(input_manager_dict.get('region_indexer'))
        instance_indexer = InstanceIndexer(input_manager_dict.get('instance_indexer'))
        copy_obj = InputManager(workflow_config, input_manager_dict.get('tail_latency_threshold'))
        copy_obj.setup(region_indexer, instance_indexer)
        return copy_obj

    def __getstate__(self):
        state = self.__dict__.copy()
        state.pop('_data_collector_client', None)
        state.pop('_region_viability_loader', None)
        state.pop('_datacenter_loader', None)
        state.pop('_performance_loader', None)
        state.pop('_carbon_loader', None)
        state.pop('_workflow_loader', None)
        state.pop('_runtime_calculator', None)
        state.pop('_carbon_calculator', None)
        state.pop('_cost_calculator', None)
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self._data_collector_client: RemoteClient = Endpoints().get_data_collector_client()
        self._region_viability_loader = RegionViabilityLoader(self._data_collector_client)
        self._datacenter_loader = DatacenterLoader(self._data_collector_client)
        self._performance_loader = PerformanceLoader(self._data_collector_client)
        self._carbon_loader = CarbonLoader(self._data_collector_client)
        self._workflow_loader = WorkflowLoader(self._data_collector_client, self._workflow_config)

        # Setup the viability loader and load the availability regions
        self._region_viability_loader.setup()  # Setup the viability loader -> This loads data from the database

        # Setup the calculator
        self._runtime_calculator = RuntimeCalculator(self._performance_loader, self._workflow_loader)
        self._carbon_calculator = CarbonCalculator(
            self._carbon_loader, self._datacenter_loader, self._workflow_loader, self._runtime_calculator
        )
        self._cost_calculator = CostCalculator(self._datacenter_loader, self._workflow_loader, self._runtime_calculator)
        requested_regions: set[str] = set(self._region_indexer.get_value_indices().keys())
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