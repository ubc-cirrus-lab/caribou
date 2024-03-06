import random
from abc import ABC, abstractmethod

from multi_x_serverless.common.constants import TAIL_LATENCY_THRESHOLD
from multi_x_serverless.routing.deployment_input.input_manager import InputManager
from multi_x_serverless.routing.models.dag import DAG
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
        tail_latency_threshold: int = TAIL_LATENCY_THRESHOLD,
    ):
        # Not all variables are relevant for other parts
        self._input_manager: InputManager = input_manager
        self._tail_latency_threshold: int = tail_latency_threshold

        # Set up the DAG structure and get the prerequisites and successor dictionaries
        dag: DAG = DAG(list(workflow_config.instances.values()), instance_indexer)
        self._prerequisites_dictionary = dag.get_prerequisites_dict()
        self._successor_dictionary = dag.get_preceeding_dict()

        # Get the home region index -> this is the region that the workflow starts from
        self._home_region_index = region_indexer.get_value_indices()[workflow_config.start_hops]

    def calculate_deployment_metrics(self, deployment: list[int], monte_carlo_runs: int = 1000) -> dict[str, float]:
        # Get average and tail cost/carbon/runtime from Monte Carlo simulation
        monte_carlo_results = self._perform_monte_carlo_simulation(deployment, monte_carlo_runs)
        tail_cost = monte_carlo_results["tail_cost"]
        tail_runtime = monte_carlo_results["tail_runtime"]
        tail_carbon = monte_carlo_results["tail_carbon"]

        average_cost = monte_carlo_results["average_cost"]
        average_runtime = monte_carlo_results["average_runtime"]
        average_carbon = monte_carlo_results["average_carbon"]

        return {
            "average_cost": average_cost,
            "average_runtime": average_runtime,
            "average_carbon": average_carbon,
            "tail_cost": tail_cost,
            "tail_runtime": tail_runtime,
            "tail_carbon": tail_carbon,
        }

    @abstractmethod
    def _perform_monte_carlo_simulation(self, deployment: list[int], times: int) -> dict[str, float]:
        """
        Perform a Monte Carlo simulation to get the average cost, runtime, and carbon footprint of the deployment.
        """
        raise NotImplementedError

    def _calculate_workflow(self, deployment: list[int], probabilistic_case: bool) -> dict[str, float]:
        total_cost = 0.0
        total_carbon = 0.0

        # Keep track of instances of the node that will get invoked in this round.
        invoked_instance_set: set = set([0])

        # Secondary dictionary to keep track of what called what
        invoked_child_dictionary: dict[int, set[int]] = {}

        # Keep track of the runtime of the instances that were invoked in this round.
        cumulative_runtime_of_instances: list[float] = [0.0] * len(deployment)

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
                    ) = self._input_manager.get_transmission_cost_carbon_runtime(
                        -1, instance_index, self._home_region_index, region_index, probabilistic_case
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
                            ) = self._input_manager.get_transmission_cost_carbon_runtime(
                                predecessor_instance_index,
                                instance_index,
                                deployment[predecessor_instance_index],
                                region_index,
                                probabilistic_case,
                            )

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
                ) = self._input_manager.get_execution_cost_carbon_runtime(
                    instance_index, region_index, probabilistic_case
                )

                total_cost += execution_cost
                total_carbon += execution_carbon
                cumulative_runtime += execution_runtime

                # Update the cumulative runtime of the instance
                cumulative_runtime_of_instances[instance_index] = cumulative_runtime

                # Determine if the next instances will be invoked
                cumulative_invoked_instance_set = set()
                for successor_instance_index in self._successor_dictionary[instance_index]:
                    if self._is_invoked(instance_index, successor_instance_index, probabilistic_case):
                        invoked_instance_set.add(successor_instance_index)
                        cumulative_invoked_instance_set.add(successor_instance_index)

                invoked_child_dictionary[instance_index] = cumulative_invoked_instance_set

        # At this point we may have 1 or more leaf nodes, we need to get the max runtime from them.
        return {
            "cost": total_cost,
            "runtime": max(cumulative_runtime_of_instances),
            "carbon": total_carbon,
        }

    def _is_invoked(self, from_instance_index: int, to_instance_index: int, probabilistic_case: bool) -> bool:
        """
        Return true if the edge would be triggered, if the probabilistic_case is True,
        It triggers dependent on the probability of the edge, if the probabilistic_case is False,
        It always triggers the edge.
        """
        if not probabilistic_case:
            return True

        invocation_probability = self._input_manager.get_invocation_probability(from_instance_index, to_instance_index)
        return random.random() < invocation_probability