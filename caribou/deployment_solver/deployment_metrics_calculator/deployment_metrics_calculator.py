import random
from abc import ABC

from caribou.common.constants import TAIL_LATENCY_THRESHOLD
from caribou.deployment_solver.deployment_input.input_manager import InputManager
from caribou.deployment_solver.deployment_metrics_calculator.models.workflow_instance import WorkflowInstance
from caribou.deployment_solver.models.dag import DAG
from caribou.deployment_solver.models.instance_indexer import InstanceIndexer
from caribou.deployment_solver.models.region_indexer import RegionIndexer
from caribou.deployment_solver.workflow_config import WorkflowConfig


class DeploymentMetricsCalculator(ABC):
    def __init__(
        self,
        workflow_config: WorkflowConfig,
        input_manager: InputManager,
        region_indexer: RegionIndexer,
        instance_indexer: InstanceIndexer,
        tail_latency_threshold: int = TAIL_LATENCY_THRESHOLD,
        record_transmission_execution_carbon: bool = False,
        consider_from_client_latency: bool = False,
    ):
        # Not all variables are relevant for other parts
        self._input_manager: InputManager = input_manager
        self._tail_latency_threshold: int = tail_latency_threshold
        self._consider_from_client_latency: bool = consider_from_client_latency

        # Set up the DAG structure and get the prerequisites and successor dictionaries
        dag: DAG = DAG(list(workflow_config.instances.values()), instance_indexer)
        self._prerequisites_dictionary = dag.get_prerequisites_dict()
        self._successor_dictionary = dag.get_preceeding_dict()
        self._topological_order = dag.topological_sort()

        # Get the home region index -> this is the region that the workflow starts from
        self._home_region_index = region_indexer.get_value_indices()[workflow_config.home_region]

        # Set the record transmission execution carbon flag
        self._record_transmission_execution_carbon = record_transmission_execution_carbon

    def calculate_deployment_metrics(self, deployment: list[int]) -> dict[str, float]:
        # Get average and tail cost/carbon/runtime from Monte Carlo simulation
        return self._perform_monte_carlo_simulation(deployment)

    def _perform_monte_carlo_simulation(self, deployment: list[int]) -> dict[str, float]:
        """
        Perform a Monte Carlo simulation to get the average cost, runtime, and carbon footprint of the deployment.
        """
        raise NotImplementedError

    def calculate_workflow(self, deployment: list[int]) -> dict[str, float]:
        # Create an new workflow instance and configure regions
        start_hop_index = self._topological_order[0] # The first instance in the topological order is the start hop
        workflow_instance = WorkflowInstance(self._input_manager, deployment, start_hop_index, self._consider_from_client_latency)

        # Build the partial workflow instance (Partial DAG)
        for instance_index in self._topological_order:
            predecessor_instance_indices = self._prerequisites_dictionary[instance_index]

            # Add the start hop if this is the first instance
            if len(predecessor_instance_indices) == 0:
                # This is the first instance, add start hop
                workflow_instance.add_start_hop(instance_index)

            # Add the node to the workflow instance
            node_invoked: bool = workflow_instance.add_node(instance_index)

            # Add the edges to the workflow
            for successor_instance_index in self._successor_dictionary[instance_index]:
                # If node was not invoked, it will not invoke the successor edges, but
                # we still need to add the edge to the workflow instance (for data transfer calculations)
                is_invoked: bool = self._is_invoked(instance_index, successor_instance_index) if node_invoked else False

                # Add the edge to the workflow instance
                workflow_instance.add_edge(instance_index, successor_instance_index, is_invoked)

        # Calculate the overall cost, runtime, and carbon footprint of the deployment
        worklflow_metrics = workflow_instance.calculate_overall_cost_runtime_carbon()

        return worklflow_metrics

    def _is_invoked(self, from_instance_index: int, to_instance_index: int) -> bool:
        invocation_probability = self._input_manager.get_invocation_probability(from_instance_index, to_instance_index)
        return random.random() < invocation_probability