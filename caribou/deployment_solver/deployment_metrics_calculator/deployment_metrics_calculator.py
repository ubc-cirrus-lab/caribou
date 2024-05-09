import multiprocessing
# import pdb
import random
from abc import ABC, abstractmethod
from multiprocessing import Process
from typing import List

from caribou.common.constants import TAIL_LATENCY_THRESHOLD
from caribou.deployment_solver.deployment_input.input_manager import InputManager
from caribou.deployment_solver.models.dag import DAG
from caribou.deployment_solver.models.instance_indexer import InstanceIndexer
from caribou.deployment_solver.models.region_indexer import RegionIndexer
from caribou.deployment_solver.workflow_config import WorkflowConfig

N_POOL = multiprocessing.cpu_count() - 1


def get_transmission_cost_carbon_latency_wrapper(
        input_manager_dict: dict,
        from_instance_index: int,
        to_instance_index: int,
        from_region_index: int,
        to_region_index: int,
) -> tuple[tuple[int, int, int, int], tuple[float, float, float]]:
    input_manager = InputManager.from_dict(input_manager_dict)
    return (
        (from_instance_index, to_instance_index, from_region_index, to_region_index),
        input_manager.get_transmission_cost_carbon_latency(
            from_instance_index,
            to_instance_index,
            from_region_index,
            to_region_index,
        )
    )


def get_execution_cost_carbon_latency_wrapper(
        input_manager_dict: dict,
        instance_index: int,
        region_index: int
) -> tuple[tuple[int, int], tuple[float, float, float]]:
    input_manager = InputManager.from_dict(input_manager_dict)
    return (
        (instance_index, region_index),
        input_manager.get_execution_cost_carbon_latency(instance_index, region_index)
    )


def calculate_workflow_task(
        input_manager: InputManager,
        tasks_list: list[dict]
):
    results = []
    for task in tasks_list:
        if task['type'] == 'transmission':
            results.append(input_manager.get_transmission_cost_carbon_latency(**task['input']))
        else:
            results.append(input_manager.get_execution_cost_carbon_latency(**task['input']))
    return results


class DeploymentMetricsCalculator(ABC):
    def __init__(
            self,
            workflow_config: WorkflowConfig,
            input_manager: InputManager,
            region_indexer: RegionIndexer,
            instance_indexer: InstanceIndexer,
            tail_latency_threshold: int = TAIL_LATENCY_THRESHOLD,
    ):
        # Not all variables are relevant for other parts
        self._input_manager: InputManager = input_manager
        self._input_manager_pool: list[InputManager] = [
            InputManager.from_dict(input_manager.get_dict()) for _ in range(N_POOL)
        ]

        self._tail_latency_threshold: int = tail_latency_threshold

        # Set up the DAG structure and get the prerequisites and successor dictionaries
        dag: DAG = DAG(list(workflow_config.instances.values()), instance_indexer)
        self._prerequisites_dictionary = dag.get_prerequisites_dict()
        self._successor_dictionary = dag.get_preceeding_dict()

        # Get the home region index -> this is the region that the workflow starts from
        self._home_region_index = region_indexer.get_value_indices()[workflow_config.home_region]

    def calculate_deployment_metrics(self, deployment: list[int]) -> dict[str, float]:
        # Get average and tail cost/carbon/runtime from Monte Carlo simulation
        return self._perform_monte_carlo_simulation(deployment)

    @abstractmethod
    def _perform_monte_carlo_simulation(self, deployment: list[int]) -> dict[str, float]:
        """
        Perform a Monte Carlo simulation to get the average cost, runtime, and carbon footprint of the deployment.
        """
        raise NotImplementedError

    def _calculate_workflow_parallel(
            self,
            transmission_input: list[tuple[int, int, int, int]],
            execution_input: list[tuple[int, int]]
    ):
        tasks = [
            {'type': 'transmission', 'input': t_input} for t_input in transmission_input
        ] + [
            {'type': 'execution', 'input': e_input} for e_input in execution_input
        ]
        pool = []
        for cpu_index in range(N_POOL):
            start_index = cpu_index * (len(tasks) // N_POOL)
            end_index = max(start_index + (len(tasks) // N_POOL), len(tasks))
            cpu_tasks = tasks[start_index:end_index]
            p = Process(target=calculate_workflow_task, args=(self._input_manager_pool[cpu_index], cpu_tasks,))
            pool.append(p)
            p.start()

        for p in pool:
            p.join()



    def _calculate_workflow(self, deployment: list[int]) -> dict[str, float]:
        total_cost = 0.0
        total_carbon = 0.0

        # Keep track of instances of the node that will get invoked in this round.
        invoked_instance_set: set = set([0])

        # Secondary dictionary to keep track of what called what
        invoked_child_dictionary: dict[int, set[int]] = {}

        # Keep track of the runtime of the instances that were invoked in this round.
        cumulative_runtime_of_instances: list[float] = [0.0] * len(deployment)

        # Multiprocessing
        transmission_cost_carbon_latency_inputs = []
        execution_cost_carbon_latency_inputs = []

        for instance_index, region_index in enumerate(deployment):
            if instance_index in invoked_instance_set:  # Only care about the invoked instances
                predecessor_instance_indices = self._prerequisites_dictionary[instance_index]
                # First deal with transmission cost/carbon/runtime
                if len(predecessor_instance_indices) == 0:
                    # This is the first instance, deal with home region transmission cost
                    transmission_cost_carbon_latency_inputs.append(
                        (
                            self._input_manager.get_dict(), -1, instance_index, self._home_region_index,
                            region_index
                        )
                    )
                else:  # This is not the first instance
                    for predecessor_instance_index in predecessor_instance_indices:
                        # Only care about the parents that invoke the current instance
                        if instance_index in invoked_child_dictionary.get(predecessor_instance_index, set()):

                            # Calculate transmission cost/carbon/runtime TO current instance
                            transmission_cost_carbon_latency_inputs.append(
                                (
                                    self._input_manager.get_dict(),
                                    predecessor_instance_index,
                                    instance_index,
                                    deployment[predecessor_instance_index],
                                    region_index,
                                )
                            )
                # Deal with execution cost/carbon/runtime
                execution_cost_carbon_latency_inputs.append(
                    (self._input_manager.get_dict(), instance_index, region_index)
                )

                # Determine if the next instances will be invoked
                cumulative_invoked_instance_set = set()
                for successor_instance_index in self._successor_dictionary[instance_index]:
                    if self._is_invoked(instance_index, successor_instance_index):
                        invoked_instance_set.add(successor_instance_index)
                        cumulative_invoked_instance_set.add(successor_instance_index)

                invoked_child_dictionary[instance_index] = cumulative_invoked_instance_set


#         pdb.set_trace()
        transmission_pool = multiprocessing.Pool(N_POOL)
        transmission_pool_result = transmission_pool.starmap(
            get_transmission_cost_carbon_latency_wrapper,
            transmission_cost_carbon_latency_inputs
        )
        transmission_pool.close()
        transmission_pool.join()

        if len(transmission_pool_result) != len(transmission_cost_carbon_latency_inputs):
            raise RuntimeError('Failed to Compute transmission_cost_carbon_latency')

        execution_pool = multiprocessing.Pool(N_POOL)
        execution_pool_result = execution_pool.starmap(
            get_execution_cost_carbon_latency_wrapper,
            execution_cost_carbon_latency_inputs
        )
        execution_pool.close()
        execution_pool.join()

        if len(execution_pool_result) != len(execution_cost_carbon_latency_inputs):
            raise RuntimeError('Failed to Compute execution_cost_carbon_latency')

        transmission_cost_carbon_latency_results = {
            input_key: output_value for input_key, output_value in transmission_pool_result
        }
        execution_cost_carbon_latency_results = {
            input_key: output_value for input_key, output_value in execution_pool_result
        }

#         pdb.set_trace()

        # Collecting results
        for instance_index, region_index in enumerate(deployment):
            if instance_index in invoked_instance_set:  # Only care about the invoked instances
                predecessor_instance_indices = self._prerequisites_dictionary[instance_index]

                # First deal with transmission cost/carbon/runtime
                cumulative_runtime = 0.0
                if len(predecessor_instance_indices) == 0:
                    # This is the first instance, deal with home region transmission cost
                    (
                        transmission_cost,
                        transmission_carbon,
                        transmission_runtime,
                    ) = transmission_cost_carbon_latency_results.get(
                        (-1, instance_index, self._home_region_index, region_index)
                    )

                    total_cost += transmission_cost
                    total_carbon += transmission_carbon

                    cumulative_runtime += transmission_runtime
                else:  # This is not the first instance
                    max_runtime = 0.0
                    for predecessor_instance_index in predecessor_instance_indices:
                        # Only care about the parents that invoke the current instance
                        if instance_index in invoked_child_dictionary.get(predecessor_instance_index, set()):
                            parent_runtime = cumulative_runtime_of_instances[predecessor_instance_index]

                            # Calculate transmission cost/carbon/runtime TO current instance
                            (
                                transmission_cost,
                                transmission_carbon,
                                transmission_runtime,
                            ) = transmission_cost_carbon_latency_results.get((
                                predecessor_instance_index,
                                instance_index,
                                deployment[predecessor_instance_index],
                                region_index,
                            ))

                            total_cost += transmission_cost
                            total_carbon += transmission_carbon
                            runtime_from_path = parent_runtime + transmission_runtime
                            max_runtime = max(max_runtime, runtime_from_path)

                    cumulative_runtime += max_runtime

                # Deal with execution cost/carbon/runtime
                (
                    execution_cost,
                    execution_carbon,
                    execution_runtime,
                ) = execution_cost_carbon_latency_results.get((instance_index, region_index))

                total_cost += execution_cost
                total_carbon += execution_carbon
                cumulative_runtime += execution_runtime

                # Update the cumulative runtime of the instance
                cumulative_runtime_of_instances[instance_index] = cumulative_runtime

                # Determine if the next instances will be invoked
                cumulative_invoked_instance_set = set()
                for successor_instance_index in self._successor_dictionary[instance_index]:
                    if self._is_invoked(instance_index, successor_instance_index):
                        invoked_instance_set.add(successor_instance_index)
                        cumulative_invoked_instance_set.add(successor_instance_index)

                invoked_child_dictionary[instance_index] = cumulative_invoked_instance_set

        # At this point we may have 1 or more leaf nodes, we need to get the max runtime from them.
        return {
            "cost": total_cost,
            "runtime": max(cumulative_runtime_of_instances),
            "carbon": total_carbon,
        }

    def _is_invoked(self, from_instance_index: int, to_instance_index: int) -> bool:
        invocation_probability = self._input_manager.get_invocation_probability(from_instance_index, to_instance_index)
        return random.random() < invocation_probability
