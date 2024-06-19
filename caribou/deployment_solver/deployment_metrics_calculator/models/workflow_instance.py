
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
        if (self.from_instance_node and not self.from_instance_node.invoked) or not self.to_instance_node:
            return None

        from_instance_id = self.from_instance_node.instance_id if self.from_instance_node else -1
        to_instance_id = self.to_instance_node.instance_id
        from_region_id = self.from_instance_node.region_id if self.from_instance_node else -1
        to_region_id = self.to_instance_node.region_id
        
        if self.conditionally_invoked:
            # This is the case where the edge is invoked conditionally
            transmission_info = self._input_manager.get_transmission_info(from_instance_id, to_instance_id, from_region_id, to_region_id, self.contain_alternative_latency)
            cumulative_runtime = transmission_info["latency"]

            # For non-starting edges, we should add the cumulative runtime of the parent node
            if self.from_instance_node is not None:
                cumulative_runtime += self.from_instance_node.get_cumulative_runtime(to_instance_id)
            
            transmission_info["cumulative_runtime"] = cumulative_runtime
        else:
            # This is the case where the edge is conditionally NOT invoked, and thus
            # will need to look at the non-execution information.
            transmission_info = self._input_manager.get_non_execution_info(from_instance_id, to_instance_id, from_region_id, to_region_id)

        return transmission_info

class InstanceNode:
    def __init__(self, input_manager: InputManager, instane_id: int) -> None:
        self._input_manager: InputManager = input_manager

        # List of edges that go from this instance to another instance
        self.from_edges: set[InstanceEdge] = set()
        self.to_edges: set[InstanceEdge] = set()

        # The region ID of the current location of the node
        self.region_id: int = -1

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

    def get_cumulative_runtime(self, successor_instance_index: int) -> float:
        # Get the cumulative runtime of the successor edge
        # If there are no specifiec runtime for the successor
        # then return the current runtime of the node (Worse case scenario)
        return self.cumulative_runtimes["sucessors"].get(successor_instance_index, self.cumulative_runtimes["current"])

    def calculate_carbon_cost_runtime(self) -> dict[str, float]:
        # Calculate the cost and carbon of the node
        # based on the input/output size and dynamodb
        # read/write capacity
        # print(f'Calculating cost and carbon for node: {self.instance_id}')
        calculated_metrics = self._input_manager.calculate_cost_and_carbon_of_instance(
            self.cumulative_runtimes['current'],
            self.region_id,
            self.cumulative_data_input_size,
            self.cumulative_data_output_size,
            self.cumulative_egress_size,
            self.cumulative_dynamodb_read_capacity,
            self.cumulative_dynamodb_write_capacity,
        )
        # print()

        return {
            "cost": calculated_metrics['cost'],
            "carbon": calculated_metrics['carbon'],
            "runtime": self.cumulative_runtimes['current'],
        }

class WorkflowInstance:
    def __init__(self, input_manager: InputManager) -> None:
        self._input_manager: InputManager = input_manager
        self._max_runtime: float = 0.0

        # The ID is the at instance index
        self._nodes: dict[int, InstanceNode] = {}

        # The ID is the (to, from) instance index
        self._edges: dict[int, dict[int, InstanceEdge]] = {}

    def add_start_hop(self, starting_instance_index: int) -> None:
        # Create a new edge that goes from the home region to the starting instance
        self._get_edge(-1, starting_instance_index)

        # # Add the edge to the edge dictionary
        # self._edges[starting_instance_index] = {-1: start_edge}

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

        # # Add the edge to the edge dictionary
        # if to_instance_index not in self._edges:
        #     self._edges[to_instance_index] = {}
        # self._edges[to_instance_index][from_instance_index] = current_edge

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
        is_sync_node: bool = len(self._edges.get(instance_index, {})) > 0
        max_edge_runtime: float = 0.0
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
                    cumulative_runtime = transmission_info["cumulative_runtime"]
                    data_transfer_size = transmission_info["data_transfer_size"]

                    # Sync node must have more than 1 predecessor, all sync edges must have a predecessor
                    if is_sync_node:
                        if incident_edge.from_instance_node == None:
                            raise ValueError(f"Sync node must have a predecessor, destination instance: {instance_index}")

                        # sync_size = transmission_info["sync_size"] # This is minimal, so we can ignore it
                        consumed_wcu = transmission_info["consumed_dynamodb_write_capacity_units"]
                        
                        # Increment the consumed write capacity at the location 
                        # of the SYNC Node (Outgoing node, Current node)
                        current_node.cumulative_dynamodb_write_capacity += consumed_wcu

                        # # Data moves from the location of the sync node to the predecessor node
                        # ## This means that the predecessor node should have the data input size incremented
                        # incident_edge.from_instance_node.cumulative_data_input_size += sync_size

                        # ## This means that the current node should have the data output size incremented
                        # current_node.cumulative_data_output_size += sync_size

                        # # If the predecessor node is in a different region than the sync node, we
                        # # should increment the egress size of the sync node
                        # if incident_edge.from_instance_node.region_id != current_node.region_id:
                        #     current_node.cumulative_egress_size += sync_size

                    # Data move from the predecessor node to the current node
                    ## This means that the current node should have the data input size incremented
                    current_node.cumulative_data_input_size += data_transfer_size

                    ## This means that the predecessor node should have the data output size incremented
                    ## If the predecessor node exists (Aka not the start node)
                    if incident_edge.from_instance_node:
                        incident_edge.from_instance_node.cumulative_data_output_size += data_transfer_size

                        # If the predecessor node is in a different region than the current node, we
                        # should increment the egress size of the predecessor node
                        if incident_edge.from_instance_node.region_id != current_node.region_id:
                            incident_edge.from_instance_node.cumulative_egress_size += data_transfer_size

                    # Calculate the cumulative runtime of the node
                    max_edge_runtime = max(max_edge_runtime, cumulative_runtime)

                    # Mark that the node was invoked
                    node_invoked = True
                else:
                    # For the non-execution case, we should get the instances that the sync node will write to
                    # This will increment the consumed write capacity of the sync node, and also the data transfer size
                    # However since the data transfer size is minimal (<1 KB normally), it has minimal impact on the cost and carbon
                    # Such that we can ignore it in terms of size of data transfer.
                    non_execution_infos = transmission_info['non_execution_info']
                    for non_execution_info in non_execution_infos:
                        simulated_predecessor_instance_id = non_execution_info["predecessor_instance_id"]
                        sync_node_instance_id = non_execution_info["sync_node_instance_id"]
                        consumed_wcu = non_execution_info["consumed_dynamodb_write_capacity_units"]

                        # Get the predecessor to successor edge
                        ## And mark that it contains alternative latency
                        simulated_edge = self._get_edge(simulated_predecessor_instance_id, sync_node_instance_id)
                        simulated_edge.contain_alternative_latency = True

                        # Get the sync node
                        ## Append the consumed write capacity to the sync node
                        sync_node = self._get_node(sync_node_instance_id)
                        sync_node.cumulative_dynamodb_write_capacity += consumed_wcu
            else:
                # TODO: Make this a proper warning
                real_node = incident_edge.from_instance_node.invoked if incident_edge.from_instance_node else False
                if real_node:
                    print("WARNING! No transmission info in edge")
                    print(f"Node: {instance_index}")
                    print(f"Conditionally Invoked: {incident_edge.conditionally_invoked}")
                    print(f"To: {incident_edge.to_instance_node.instance_id}")

                    if incident_edge.from_instance_node:
                        print(f"From: {incident_edge.from_instance_node.instance_id}")
                        print(f"Real Edge: {incident_edge.from_instance_node.invoked}")
                    
                    print('\n')

        # Calculate the cumulative runtime of the node
        # Only if the node was invoked
        if node_invoked:
            self.cumulative_runtimes = self._input_manager.get_node_runtimes(instance_index, region_index, cumulative_runtime)

        # Set the node invoked flag
        current_node.invoked = node_invoked

    def calculate_overall_cost_runtime_carbon(self) -> dict[str, float]:
        cumulative_cost = 0.0
        cumulative_carbon = 0.0
        max_runtime = 0.0
        for node in self._nodes.values():
            node_carbon_cost_runtime = node.calculate_carbon_cost_runtime()
            cumulative_cost += node_carbon_cost_runtime["cost"]
            cumulative_carbon += node_carbon_cost_runtime["carbon"]
            max_runtime = max(max_runtime, node_carbon_cost_runtime["runtime"])

        # print(self._nodes)

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
        if to_instance_index not in self._edges:
            self._edges[to_instance_index] = {}

        if from_instance_index not in self._edges[to_instance_index]:
            # Create new edge
            edge = InstanceEdge(self._input_manager)
            self._edges[to_instance_index][from_instance_index] = edge
        
        return self._edges[to_instance_index][from_instance_index]
