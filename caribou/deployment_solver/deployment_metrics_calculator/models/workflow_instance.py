
from typing import Optional
import numpy as np

from caribou.deployment_solver.deployment_input.input_manager import InputManager
# from caribou.deployment_solver.deployment_metrics_calculator.models.instance_node import InstanceNode
# from caribou.deployment_solver.deployment_metrics_calculator.models.instance_edge import InstanceEdge

class InstanceEdge:
    # If the edge is from the home region, from_instance_id 
    # and from_region_id is None
    from_instance_id: Optional[int]
    from_region_id: Optional[int]

    # Whether the edge is
    # invoked conditionally.
    invoked: bool
    def __init__(self, to_instance_id: int) -> None:
        self.from_instance_id = None
        self.from_region_id = None

        # Edge always goes to an instance
        self.to_instance_id = to_instance_id

class InstanceNode:
    region_id: int

    def __init__(self, instane_id: int) -> None:
        # List of edges that go from this instance to another instance
        self.from_edges: set[InstanceEdge] = []
        self.to_edges: set[InstanceEdge] = []

        # The instance index
        self.instance_id = instane_id

        # Denotes if it was invoked (Any of its incoming edges were invoked)
        # Default is False
        self.invoked: bool = False
        
class WorkflowInstance:
    def __init__(self, input_manager: InputManager) -> None:
        self._input_manager: InputManager = input_manager
        self._max_runtime: float = 0.0

        # The ID is the instance index
        self._nodes: dict[int, InstanceNode] = {}

        # The ID is the to instance index
        self._edges: dict[int, set[InstanceEdge]] = {}

        # # Start node ID
        # self._start_node_id: int = -1

    def add_start_hop(self, starting_instance_index: int) -> None:
        # Create a new edge that goes from the home region to the starting instance
        start_edge: InstanceEdge = self._get_edge(starting_instance_index)

        # Add the edge to the edge dictionary
        self._edges[starting_instance_index] = set([start_edge])

        # # Denote that the the start node has the instance index
        # self._start_node_id = starting_instance_index

    def add_edge(self, from_instance_index: int, to_instance_index: int, invoked: bool) -> None:
        # Get the from node
        # We need this to determine if the edge is actually
        # invoked or not, as if the from node is not invoked
        # then the edge is not invoked.
        from_node: InstanceNode = self._get_node(from_instance_index)
        invoked = invoked and from_node.invoked

        # Get the edge
        current_edge: InstanceEdge = self._get_edge(to_instance_index)

        # Add the other aspects of the edge
        current_edge.from_instance_id = from_instance_index
        current_edge.from_region_id = self._nodes[from_instance_index].region_id
        current_edge.invoked = invoked

        # Add the edge to the from edges of the from node
        from_node.to_edges.add(current_edge)

        # Add the edge to the edge dictionary
        if to_instance_index not in self._edges:
            self._edges[to_instance_index] = set()
        self._edges[to_instance_index].add(current_edge)

    def add_node(self, instance_index: int, region_index: int) -> None:
        '''
        Add a new node to the workflow instance.
        This function will also link all the edges that go to this node.
        And calculate and materialize the cumulative runtime of the node.
        And also the cost and carbon of the edge to the node.
        '''
        # Create a new node
        current_node = InstanceNode(instance_index, region_index)

        # Link all the edges that go to this node
        for incident_edge in self._edges.get(instance_index, []):
            current_node.from_edges.add(incident_edge)

        # Calculate the cumulative runtime of the node
        self._nodes[instance_index] = current_node

    def calculate_overall_cost_runtime_carbon(self, deployment: list[int]) -> dict[str, float]:
        # Get average and tail cost/carbon/runtime from Monte Carlo simulation
        # return self._perform_monte_carlo_simulation(deployment)
        cost = 0.0
        carbon = 0.0
        return {
            "cost": cost,
            "runtime": self._max_runtime,
            "carbon": carbon,
        }

    def _get_node(self, instance_index: int) -> InstanceNode:
        # Get node if exists, else create a new node
        if instance_index not in self._nodes:
            # Create new node
            node = InstanceNode(instance_index)
            self._nodes[instance_index] = node
        
        return self._nodes[instance_index]

    def _get_edge(self, to_instance_index: int) -> InstanceEdge:
        # Get edge if exists, else create a new edge
        if to_instance_index not in self._edges:
            # Create new edge
            edge = InstanceEdge(to_instance_index)
            self._edges[to_instance_index] = edge
        
        return self._edges[to_instance_index]


    # def calculate_workflow(self, deployment: list[int]) -> dict[str, float]:
    #     total_cost = 0.0
    #     total_carbon = 0.0

    #     # Keep track of instances of the node that will get invoked in this round.
    #     invoked_instance_set: set = set([0])

    #     # Secondary dictionary to keep track of what called what
    #     invoked_child_dictionary: dict[int, set[int]] = {}

    #     # Keep track of the runtime of the instances that were invoked in this round.
    #     cumulative_runtime_of_instances: list[float] = [0.0] * len(deployment)

    #     for instance_index in self._topological_order:
    #         region_index = deployment[instance_index]
    #         if instance_index in invoked_instance_set:  # Only care about the invoked instances
    #             predecessor_instance_indices = self._prerequisites_dictionary[instance_index]

    #             # First deal with transmission cost/carbon/runtime
    #             cumulative_runtime = 0.0
    #             if len(predecessor_instance_indices) == 0:
    #                 # This is the first instance, deal with home region transmission cost
    #                 (
    #                     transmission_cost,
    #                     transmission_carbon,
    #                     transmission_runtime,
    #                 ) = self._input_manager.get_transmission_cost_carbon_latency(
    #                     -1, instance_index, self._home_region_index, region_index
    #                 )

    #                 total_cost += transmission_cost
    #                 total_carbon += transmission_carbon

    #                 cumulative_runtime += transmission_runtime
    #             else:  # This is not the first instance
    #                 max_runtime = 0.0
    #                 for predecessor_instance_index in predecessor_instance_indices:
    #                     # Only care about the parents that invoke the current instance
    #                     if instance_index in invoked_child_dictionary.get(predecessor_instance_index, set()):
    #                         parent_runtime = cumulative_runtime_of_instances[predecessor_instance_index]

    #                         # Calculate transmission cost/carbon/runtime TO current instance
    #                         (
    #                             transmission_cost,
    #                             transmission_carbon,
    #                             transmission_runtime,
    #                         ) = self._input_manager.get_transmission_cost_carbon_latency(
    #                             predecessor_instance_index,
    #                             instance_index,
    #                             deployment[predecessor_instance_index],
    #                             region_index,
    #                         )

    #                         total_cost += transmission_cost
    #                         total_carbon += transmission_carbon
    #                         runtime_from_path = parent_runtime + transmission_runtime
    #                         max_runtime = max(max_runtime, runtime_from_path)

    #                 cumulative_runtime += max_runtime

    #             # Deal with execution cost/carbon/runtime
    #             (
    #                 execution_cost,
    #                 execution_carbon,
    #                 execution_runtime,
    #             ) = self._input_manager.get_execution_cost_carbon_latency(instance_index, region_index)

    #             total_cost += execution_cost
    #             total_carbon += execution_carbon
    #             cumulative_runtime += execution_runtime

    #             # Update the cumulative runtime of the instance
    #             cumulative_runtime_of_instances[instance_index] = cumulative_runtime

    #             # Determine if the next instances will be invoked
    #             cumulative_invoked_instance_set = set()
    #             for successor_instance_index in self._successor_dictionary[instance_index]:
    #                 if self._is_invoked(instance_index, successor_instance_index):
    #                     invoked_instance_set.add(successor_instance_index)
    #                     cumulative_invoked_instance_set.add(successor_instance_index)

    #             invoked_child_dictionary[instance_index] = cumulative_invoked_instance_set

    #     # At this point we may have 1 or more leaf nodes, we need to get the max runtime from them.
    #     return {
    #         "cost": total_cost,
    #         "runtime": max(cumulative_runtime_of_instances),
    #         "carbon": total_carbon,
    #     }