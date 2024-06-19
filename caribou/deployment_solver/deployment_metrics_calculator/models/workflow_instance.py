
from typing import Any, Optional
import numpy as np

from caribou.deployment_solver.deployment_input.input_manager import InputManager
# from caribou.deployment_solver.deployment_metrics_calculator.models.instance_node import InstanceNode
# from caribou.deployment_solver.deployment_metrics_calculator.models.instance_edge import InstanceEdge

class InstanceNode:
    pass

class InstanceEdge:
    def __init__(self, input_manager: InputManager) -> None:
        self._input_manager: InputManager = input_manager

        # Edge always goes to an instance 
        self.from_instance_node: Optional[InstanceNode] = None
        self.to_instance_node: Optional[InstanceNode] = None

        # Whether the edge is
        # invoked conditionally.
        self.conditionally_invoked: bool = False

        # Store the alternative latency of the edge
        # Only from non-execution of ancestor nodes and only
        # if current to_instance node is a sync node.
        # We need to take the max of the alternative latency and
        # the normal latency to get the actual latency
        self.contain_alternative_latency: bool = False

    def get_transmission_information(self) -> Optional[dict[str, Any]]:
        # Check the edge if it is a real edge
        # First get the parent node
        # If the parent node is invoked, then the edge is real
        # If the parent node is not invoked, then the edge is not real
        # If the edge is NOT real, then we should return None
        if not self.from_instance_node or not self.from_instance_node.invoked or not self.to_instance_node:
            return None

        from_instance_id = self.from_instance_node.instance_id if self.from_instance_node else -1
        to_instance_id = self.to_instance_node.instance_id
        from_region_id = self.from_instance_node.region_id if self.from_instance_node else -1
        to_region_id = self.to_instance_node.region_id
        
        if self.conditionally_invoked:
            # This is the case where the edge is invoked conditionally
            transmission_info = self._input_manager.get_transmission_info(from_instance_id, to_instance_id, from_region_id, to_region_id, self.contain_alternative_latency)
        else:
            # This is the case where the edge is conditionally NOT invoked, and thus
            # will need to look at the non-execution information.
            transmission_info = self._input_manager.get_non_execution_info(from_instance_id, to_instance_id, from_region_id, to_region_id)

        return transmission_info

class InstanceNode:
    region_id: int

    def __init__(self, input_manager: InputManager, instane_id: int) -> None:
        self._input_manager: InputManager = input_manager

        # List of edges that go from this instance to another instance
        self.from_edges: set[InstanceEdge] = []
        self.to_edges: set[InstanceEdge] = []

        # The instance index
        self.instance_id = instane_id

        # Denotes if it was invoked (Any of its incoming edges were invoked)
        # Default is False
        self.invoked: bool = False

        # Store the cumulative data input and
        # output size of the node
        self.cumulative_data_input_size: float = 0.0
        self.cumulative_data_output_size: float = 0.0

        # Store the cumulative data output size out of the current region
        self.cumulative_egress_size: float = 0.0

        # Store the cumulative dynamodb read and
        # write capacity of the node
        self.cumulative_dynamodb_read_capacity: float = 0.0
        self.cumulative_dynamodb_write_capacity: float = 0.0

        # Store the cumulative runtime of the node
        self.cumulative_runtimes: dict[str, Any] = {
            "current": 0.0,
            "sucessors": {
                # The key is the instance index of the successor
                # The value is the cumulative runtime of when this
                # node invokes the successor
            }
        }

    def calculate_carbon_cost_runtime(self) -> dict[str, float]:
        # Calculate the cost and carbon of the node
        # based on the input/output size and dynamodb
        # read/write capacity
        cost = 0.0
        carbon = 0.0
        if self.invoked:
            # Process and calculate the cost and carbon
            # From input and output data transfer and dynamodb capacity
            pass
        
        return {
            "cost": cost,
            "carbon": carbon,
            "runtime": self.cumulative_runtimes['current'],
        }

class WorkflowInstance:
    def __init__(self, input_manager: InputManager) -> None:
        self._input_manager: InputManager = input_manager
        self._max_runtime: float = 0.0

        # The ID is the at instance index
        self._nodes: dict[int, InstanceNode] = {}

        # The ID is the from, to instance index
        self._edges: dict[int, dict[int, InstanceEdge]] = {}

    def add_start_hop(self, starting_instance_index: int) -> None:
        # Create a new edge that goes from the home region to the starting instance
        start_edge: InstanceEdge = self._get_edge(-1, starting_instance_index)

        # Add the edge to the edge dictionary
        self._edges[-1] = {starting_instance_index: start_edge}

    def add_edge(self, from_instance_index: int, to_instance_index: int, invoked: bool) -> None:
        # Get the from node
        # We need this to determine if the edge is actually
        # invoked or not, as if the from node is not invoked
        # then the edge is not invoked.
        from_node: InstanceNode = self._get_node(from_instance_index)
        invoked = invoked and from_node.invoked

        # Get the edge (Create if not exists)
        current_edge: InstanceEdge = self._get_edge(from_instance_index, to_instance_index)

        # Add the other aspects of the edge
        current_edge.from_instance_node = from_node
        # current_edge.from_region_id = self._nodes[from_instance_index].region_id
        current_edge.conditionally_invoked = invoked

        # Add the edge to the from edges of the from node
        from_node.to_edges.add(current_edge)

        # Add the edge to the edge dictionary
        if to_instance_index not in self._edges:
            self._edges[to_instance_index] = {}
        self._edges[to_instance_index][from_instance_index] = current_edge

    def add_node(self, instance_index: int, region_index: int) -> None:
        '''
        Add a new node to the workflow instance.
        This function will also link all the edges that go to this node.
        And calculate and materialize the cumulative runtime of the node.
        And also the cost and carbon of the edge to the node.
        '''
        # Create a new node (This will always create a new node if
        # this class is configured correctly)
        current_node = self._get_node(instance_index)
        current_node.region_id = region_index

        # Process, materialize, then
        # Link all the edges that go to this node
        node_invoked: bool = False
        for incident_edge in self._edges.get(instance_index, {}).values():
            current_node.from_edges.add(incident_edge)
            incident_edge.to_instance_node = current_node

            # Materialize the edge
            transmission_info = incident_edge.get_transmission_information()
            if transmission_info:
                if incident_edge.conditionally_invoked:
                    # For the normal execution case, we should get the size of data transfer, and the transfer latency.
                    # If this node is a sync predecessor, we should also retrieve the sync_sizes_gb this denotes
                    # the size of the sync table that need to be updated (invoked twice) to its successor, and also indicates
                    # that the edge is a sync edge, and thus it transfer the data through the sync table rather than directly through SNS.
                    pass
                else:
                    # For the non-execution case, we should get the instances that the sync node will write to
                    # This will increment the consumed write capacity of the sync node, and also the data transfer size
                    # However since the data transfer size is minimal (<1 KB normally), we should not include it in the cost calculation


                    # parsed_sync_to_from_instance = sync_to_from_instance.split(">")
                    # sync_predecessor_instance = parsed_sync_to_from_instance[0]
                    # sync_node_instance = parsed_sync_to_from_instance[1]
                    pass


                pass

                # Calculate the properties of the edge
                # This means it should get all the data
                # input/output size, dynamodb read/write capacity
                # To its direct successor. However if it is a real
                # edge (An edge is real if its parent node is invoked)
                # And it applies a non-execution, then the edge transfers
                # data and consumes dynamodb read/write OUTSIDE of the 
                # direct successor. In this case we should return a
                # seperate dictionary denoting this.




        # Calculate the cumulative runtime of the node
        # Only if the node was invoked
        if node_invoked:
            # Calculate the cumulative runtime of the node
            # current_node.cumulative_runtime = 0.0
            for incident_edge in current_node.from_edges:
                # Calculate the cumulative runtime of the node
                pass


        # Add the node to the node dictionary
        self._nodes[instance_index] = current_node

    def calculate_overall_cost_runtime_carbon(self) -> dict[str, float]:
        cumulative_cost = 0.0
        cumulative_carbon = 0.0
        max_runtime = 0.0
        for node in self._nodes.values():
            node_carbon_cost_runtime = node.calculate_carbon_cost_runtime()
            cumulative_cost += node_carbon_cost_runtime["cost"]
            cumulative_carbon += node_carbon_cost_runtime["carbon"]
            max_runtime = max(max_runtime, node_carbon_cost_runtime["runtime"])
        
        return {
            "cost": cumulative_cost,
            "runtime": max_runtime,
            "carbon": cumulative_carbon,
        }

    def _get_node(self, instance_index: int) -> InstanceNode:
        # Get node if exists, else create a new node
        if instance_index not in self._nodes:
            # Create new node
            node = InstanceNode(self._input_manager, instance_index)
            self._nodes[instance_index] = node
        
        return self._nodes[instance_index]

    def _get_edge(self, from_instance_index: int, to_instance_index: int) -> InstanceEdge:
        # Get edge if exists, else create a new edge
        if from_instance_index not in self._edges:
            self._edges[from_instance_index] = {}

        if to_instance_index not in self._edges[from_instance_index]:
            # Create new edge
            edge = InstanceEdge(self._input_manager, to_instance_index)
            self._edges[from_instance_index][to_instance_index] = edge
        
        return self._edges[from_instance_index][to_instance_index]