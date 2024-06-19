
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

    def get_transmission_information(self, successor_is_sync_node: bool) -> Optional[dict[str, Any]]:
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
            cumulative_runtime = 0.0
            # For non-starting edges, we should add the cumulative runtime of the parent node
            if self.from_instance_node is not None:
                cumulative_runtime += self.from_instance_node.get_cumulative_runtime(to_instance_id)

            transmission_info = self._input_manager.get_transmission_info(from_instance_id, from_region_id, to_instance_id, to_region_id, cumulative_runtime, self.contain_alternative_latency, successor_is_sync_node)
        else:
            # This is the case where the edge is conditionally NOT invoked, and thus
            # will need to look at the non-execution information.
            transmission_info = self._input_manager.get_non_execution_info(from_instance_id, to_instance_id)

        return transmission_info

class InstanceNode:
    def __init__(self, input_manager: InputManager, instane_id: int) -> None:
        self._input_manager: InputManager = input_manager

        # # List of edges that go from this instance to another instance
        # self.from_edges: set[InstanceEdge] = set()
        # self.to_edges: set[InstanceEdge] = set()

        # The region ID of the current location of the node
        self.region_id: int = -1

        # The instance index
        self.instance_id = instane_id

        # Denotes if it was invoked (Any of its incoming edges were invoked)
        # Default is False
        self.invoked: bool = False

        # Store the cumulative data input and
        # output size of the node (Include sns_data_output_sizes as well)
        self.tracked_data_input_sizes: dict[int, float] = {}
        self.tracked_data_output_sizes: dict[int, float] = {}
        
        ## This stores the data_transfer during execution where
        # we CANNOT account for where its from or to
        self.data_transfer_during_execution: float = 0.0

        # Store the cumulative data output size of only SNS
        self.sns_data_output_sizes: dict[int, float] = {}

        # Store the cumulative dynamodb read and
        # write capacity of the node
        self.tracked_dynamodb_read_capacity: float = 0.0
        self.tracked_dynamodb_write_capacity: float = 0.0

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
            self.instance_id,
            self.region_id,
            self.tracked_data_input_sizes,
            self.tracked_data_output_sizes,
            self.sns_data_output_sizes,
            self.data_transfer_during_execution,
            self.tracked_dynamodb_read_capacity,
            self.tracked_dynamodb_write_capacity,
        )
        # print()

        return {
            "cost": calculated_metrics['cost'],
            "carbon": calculated_metrics['carbon'],
            "runtime": self.cumulative_runtimes['current'],
        }

class WorkflowInstance:
    def __init__(self, input_manager: InputManager, instance_deployment_regions: list[int]) -> None:
        self._input_manager: InputManager = input_manager
        self._max_runtime: float = 0.0

        # The ID is the at instance index
        self._nodes: dict[int, InstanceNode] = {}

        # The ID is the (to, from) instance index
        self._edges: dict[int, dict[int, InstanceEdge]] = {}

        # Configure the regions of the nodes
        self._configure_node_regions(instance_deployment_regions)

    def _configure_node_regions(self, instance_deployment_regions: list[int]) -> None:
        for instance_index in range(len(instance_deployment_regions)):
            region_index = instance_deployment_regions[instance_index]
            current_node = self._get_node(instance_index)
            current_node.region_id = region_index

    def add_start_hop(self, starting_instance_index: int) -> None:
        # Create a new edge that goes from the home region to the starting instance
        start_hop_edge = self._get_edge(-1, starting_instance_index)
        start_hop_edge.conditionally_invoked = True

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

        # # Add the edge to the from edges of the from node
        # from_node.to_edges.add(current_edge)

        # # Add the edge to the edge dictionary
        # if to_instance_index not in self._edges:
        #     self._edges[to_instance_index] = {}
        # self._edges[to_instance_index][from_instance_index] = current_edge

    def add_node(self, instance_index: int) -> None:
        '''
        Add a new node to the workflow instance.
        This function will also link all the edges that go to this node.
        And calculate and materialize the cumulative runtime of the node.
        And also the cost and carbon of the edge to the node.
        '''
        # Create a new node (This will always create a new node if
        # this class is configured correctly)
        current_node = self._get_node(instance_index)

        # Process, materialize, then
        # Link all the edges that go to this node
        node_invoked: bool = False
        is_sync_node: bool = len(self._edges.get(instance_index, {})) > 1
        max_edge_runtime: float = 0.0
        sync_edge_upload_edges_auxiliary_data: list[tuple[float, float, float]] = []
        for incident_edge in self._edges.get(instance_index, {}).values():
            # current_node.from_edges.add(incident_edge)
            incident_edge.to_instance_node = current_node

            # Materialize the edge
            transmission_info = incident_edge.get_transmission_information(is_sync_node)
            if transmission_info:
                if incident_edge.conditionally_invoked:
                    # For the normal execution case, we should get the size of data transfer, and the transfer latency.
                    # If this node is a sync predecessor, we should also retrieve the sync_sizes_gb this denotes
                    # the size of the sync table that need to be updated (invoked twice) to its successor, and also indicates
                    # that the edge is a sync edge, and thus it transfer the data through the sync table rather than directly through SNS.
                    cumulative_runtime = transmission_info["cumulative_runtime"]
                    sns_data_transfer_size = transmission_info["sns_data_transfer_size"]

                    ## This means that the predecessor node should have the data output size incremented
                    ## If the predecessor node exists (Aka not the start node)
                    from_region_id = -1
                    if incident_edge.from_instance_node:
                        from_region_id = incident_edge.from_instance_node.region_id
                        self._manage_data_transfer_dict(incident_edge.from_instance_node.tracked_data_output_sizes, from_region_id, sns_data_transfer_size)

                    # Data move from the predecessor node to the current node
                    ## This means that the current node should have the data input size incremented
                    self._manage_data_transfer_dict(current_node.tracked_data_input_sizes, from_region_id, sns_data_transfer_size)

                    # Payload is uploaded to the SNS
                    self._manage_data_transfer_dict(current_node.sns_data_output_sizes, from_region_id, sns_data_transfer_size)

                    # Sync node must have more than 1 predecessor, all sync edges must have a predecessor
                    # For sync nodes, payload is uploaded to the sync table and not through SNS
                    if is_sync_node:
                        if incident_edge.from_instance_node == None:
                            raise ValueError(f"Sync node must have a predecessor, destination instance: {instance_index}")
                        sync_info = transmission_info["sync_info"]
                        sync_size = sync_info["sync_size"]
                        consumed_wcu = sync_info["consumed_dynamodb_write_capacity_units"]
                        dynamodb_upload_size = sync_info["dynamodb_upload_size"]
                        sync_upload_auxiliary_info = sync_info["sync_upload_auxiliary_info"]
                        sync_edge_upload_edges_auxiliary_data.append(sync_upload_auxiliary_info)

                        # Increment the consumed write capacity at the location 
                        # of the SYNC Node (Outgoing node, Current node)
                        current_node.tracked_dynamodb_write_capacity += consumed_wcu

                        # Data denoted by dynamodb_upload_size is uploaded to the sync table (Move out of the predecessor node, and into the current, sync node)
                        self._manage_data_transfer_dict(incident_edge.from_instance_node.tracked_data_output_sizes, current_node.region_id, dynamodb_upload_size)
                        self._manage_data_transfer_dict(current_node.tracked_data_input_sizes, incident_edge.from_instance_node.region_id, dynamodb_upload_size)

                        # Data moves from the location of the sync node to the predecessor node
                        ## This means that the predecessor node should have the data input size incremented
                        ## This means that the current node should have the data output size incremented
                        self._manage_data_transfer_dict(incident_edge.from_instance_node.tracked_data_input_sizes, current_node.region_id, sync_size)
                        self._manage_data_transfer_dict(current_node.tracked_data_output_sizes, incident_edge.from_instance_node.region_id, sync_size)


                    # Calculate the cumulative runtime of the node
                    max_edge_runtime = max(max_edge_runtime, cumulative_runtime)

                    # Mark that the node was invoked
                    node_invoked = True
                else:
                    # For the non-execution case, we should get the instances that the sync node will write to
                    # This will increment the consumed write capacity of the sync node, and also the data transfer size
                    non_execution_infos = transmission_info['non_execution_info']
                    for non_execution_info in non_execution_infos:
                        simulated_predecessor_instance_id = non_execution_info["predecessor_instance_id"]
                        sync_node_instance_id = non_execution_info["sync_node_instance_id"]
                        consumed_wcu = non_execution_info["consumed_dynamodb_write_capacity_units"]
                        sync_data_response_size = non_execution_info["sync_data_response_size"]

                        # Get the predecessor to successor edge
                        ## And mark that it contains alternative latency
                        simulated_edge = self._get_edge(simulated_predecessor_instance_id, sync_node_instance_id)
                        simulated_edge.contain_alternative_latency = True

                        # Get the sync node
                        ## Append the consumed write capacity to the sync node
                        sync_node = self._get_node(sync_node_instance_id)
                        sync_node.tracked_dynamodb_write_capacity += consumed_wcu

                        # Data of sync_data_response_size is moved from the sync node to the current node
                        self._manage_data_transfer_dict(sync_node.tracked_data_output_sizes, current_node.region_id, sync_data_response_size)
                        self._manage_data_transfer_dict(current_node.tracked_data_input_sizes, sync_node.region_id, sync_data_response_size)
            else:
                real_node = incident_edge.from_instance_node.invoked if incident_edge.from_instance_node else False
                if real_node:
                    # In the case the edge is real, there should never be a case where
                    # there are no transmission information
                    raise ValueError(
                        f"No transmission info in edge",
                        f"Node: {instance_index}",
                        f"Conditionally Invoked: {incident_edge.conditionally_invoked}",
                        f"To: {incident_edge.to_instance_node.instance_id}",
                    )

        # Handle sync upload auxiliary data
        ## This the write capacity unit of the sync node
        if len(sync_edge_upload_edges_auxiliary_data) > 0:
            capacity_units = self._input_manager.calculate_dynamodb_capacity_unit_of_sync_edges(sync_edge_upload_edges_auxiliary_data)
            current_node.tracked_dynamodb_write_capacity += capacity_units['write_capacity_units']
            current_node.tracked_dynamodb_read_capacity += capacity_units['read_capacity_units']

        # Calculate the cumulative runtime of the node
        # Only if the node was invoked
        if node_invoked:
            self.cumulative_runtimes, data_transfer_during_execution = self._input_manager.get_node_runtimes_and_data_transfer(instance_index, current_node.region_id, cumulative_runtime)
            
            # Handle the data transfer during execution
            # We will asume the data comes from the same region as the node
            current_node.data_transfer_during_execution += data_transfer_during_execution

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

    def _manage_data_transfer_dict(self, data_transfer_dict: dict[int, float], region_id: int, data_transfer_size: float) -> None:
        if region_id not in data_transfer_dict:
            data_transfer_dict[region_id] = 0.0
        
        data_transfer_dict[region_id] += data_transfer_size