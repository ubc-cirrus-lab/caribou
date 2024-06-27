from typing import Any, Optional, Union

import numpy as np

from caribou.deployment_solver.deployment_input.input_manager import InputManager
from caribou.deployment_solver.deployment_metrics_calculator.models.instance_edge import InstanceEdge
from caribou.deployment_solver.deployment_metrics_calculator.models.instance_node import InstanceNode
from caribou.deployment_solver.deployment_metrics_calculator.models.simulated_instance_edge import SimulatedInstanceEdge


class WorkflowInstance:
    def __init__(self, input_manager: InputManager, instance_deployment_regions: list[int]) -> None:
        self._input_manager: InputManager = input_manager

        # The ID is the at instance index
        self._nodes: dict[int, InstanceNode] = {}

        # The ID is the (to, from) instance index
        self._edges: dict[int, dict[int, InstanceEdge]] = {}

        # Simulated edges are edges that are not actually apart of the workflow
        # but are used to handle latencies of non-execution of ancestor nodes
        self._simulated_edges: dict[int, dict[int, SimulatedInstanceEdge]] = {}

        # Configure the regions of the nodes
        self._configure_node_regions(instance_deployment_regions)

    def _configure_node_regions(self, instance_deployment_regions: list[int]) -> None:
        for instance_index in range(len(instance_deployment_regions)):
            region_index = instance_deployment_regions[instance_index]
            current_node = self._get_node(instance_index)
            current_node.region_id = region_index

    def add_start_hop(self, starting_instance_index: int) -> None:
        # Create a new edge that goes from the home region to the starting instance
        start_hop_edge: InstanceEdge = self._create_edge(-1, starting_instance_index)
        start_hop_edge.conditionally_invoked = True

    def add_edge(self, from_instance_index: int, to_instance_index: int, invoked: bool) -> None:
        # Get the from node
        # We need this to determine if the edge is actually
        # invoked or not, as if the from node is not invoked
        # then the edge is not invoked.
        from_node: InstanceNode = self._get_node(from_instance_index)

        # Get the edge (Create if not exists)
        current_edge: InstanceEdge = self._create_edge(from_instance_index, to_instance_index)

        # A edge can only be conditionally invoked if BOTH the
        # previous node and the current edge is designated as invoked
        current_edge.conditionally_invoked = invoked and from_node.invoked

    def add_node(self, instance_index: int) -> bool:
        """
        Add a new node to the workflow instance.
        This function will also link all the edges that go to this node.
        And calculate and materialize the cumulative runtime of the node.
        And also the cost and carbon of the edge to the node.
        Return if the workflow instance was invoked.
        """
        # Create a new node (This will always create a new node if
        # this class is configured correctly)
        current_node = self._get_node(instance_index)

        # Look through all the real edges linking to this node
        node_invoked: bool = False
        real_predecessor_edges: list[InstanceEdge] = self._get_predecessor_edges(instance_index, False)
        is_sync_node: bool = len(real_predecessor_edges) > 1
        sync_edge_upload_data: list[tuple[float, float]] = []
        edge_reached_time_to_sns_data: list[tuple[float, dict[str, Any]]] = []
        for current_edge in real_predecessor_edges:
            current_node_invoked = self._handle_real_edge(
                current_edge, is_sync_node, sync_edge_upload_data, edge_reached_time_to_sns_data
            )
            node_invoked = node_invoked or current_node_invoked

            from_instance_id = current_edge.from_instance_node.instance_id if current_edge.from_instance_node else -1
            # print(
            #     f"WI: Processing Real Edge, from: {from_instance_id} -> {current_edge.to_instance_node.instance_id} -> {current_node_invoked}"
            # )

        # Handle sync upload auxiliary data
        ## This the write capacity unit of the sync node
        # Purely from data upload and download, where upload
        # is done via UpdateItem (Which consumes whole write
        # capacity unit of size of the table)
        if len(sync_edge_upload_data) > 0:
            capacity_units = self._input_manager.calculate_dynamodb_capacity_unit_of_sync_edges(sync_edge_upload_data)
            current_node.tracked_dynamodb_write_capacity += capacity_units["write_capacity_units"]
            current_node.tracked_dynamodb_read_capacity += capacity_units["read_capacity_units"]

        # Calculate the cumulative runtime of the  node
        # Only if the node was invoked
        if node_invoked:
            # We only care about simulated edges IFF the node was invoked
            # As it determines the actual runtime of the node (and represent SNS call)
            simulated_predecessor_edges: list[SimulatedInstanceEdge] = self._get_predecessor_edges(instance_index, True)
            for simulated_edge in simulated_predecessor_edges:
                # print(
                #     f"WI: Processing Simulated Edge, from: {simulated_edge.from_instance_node.instance_id} -> {simulated_edge.to_instance_node.instance_id}"
                # )
                self._handle_simulated_edge(simulated_edge, edge_reached_time_to_sns_data)

            # Calculate the cumulative runtime of the node and the data transfer during execution
            cumulative_runtime = self._handle_sns_invocation(edge_reached_time_to_sns_data)
            # print(f"WI: Runtime before execution: {cumulative_runtime} s")
            (
                current_node.cumulative_runtimes,
                data_transfer_during_execution,
            ) = self._input_manager.get_node_runtimes_and_data_transfer(
                instance_index, current_node.region_id, cumulative_runtime
            )
            # print(f"WI: Runtimes after execution: {current_node.cumulative_runtimes} s")

            # Handle the data transfer during execution
            # We will asume the data comes from the same region as the node
            current_node.data_transfer_during_execution += data_transfer_during_execution

        # Set the node invoked flag
        current_node.invoked = node_invoked

        return node_invoked

    def _handle_sns_invocation(self, edge_reached_time_to_sns_data: list[tuple[float, dict[str, Any]]]) -> float:
        # Sort the sns data by the starting runtime, aka when it
        # was invoked, we only want the one with the longest runtime
        # As that is the one that will actually cause the SNS invocation
        edge_reached_time_to_sns_data.sort(key=lambda x: x[0], reverse=True)

        parsed_dict: list[tuple[int, int]] = []
        for _, (start_runtime, sns_data) in enumerate(edge_reached_time_to_sns_data):
            from_instance_id = sns_data["from_instance_node"].instance_id if sns_data["from_instance_node"] else -1
            to_instance_id = sns_data["to_instance_node"].instance_id
            parsed_dict.append((f"{from_instance_id}>{to_instance_id}", start_runtime, sns_data["cumulative_runtime"]))
        # print(f"WIT: Edge Reached Time to SNS Data: {parsed_dict}")

        if len(edge_reached_time_to_sns_data) > 0:
            # Get the edge that will invoke the SNS
            sns_edge_data = edge_reached_time_to_sns_data[0][1]
            cumulative_runtime: float = sns_edge_data["cumulative_runtime"]
            sns_data_transfer_size: float = sns_edge_data["sns_data_transfer_size"]
            from_instance_node: Optional[InstanceNode] = sns_edge_data["from_instance_node"]
            to_instance_node: InstanceNode = sns_edge_data["to_instance_node"]

            ## This means that the predecessor node should have the data output size incremented
            ## If the predecessor node exists (Aka not the start node)
            from_region_id = -1
            # If the from_instance_node is None, then the edge is the start hop
            # and thus the data is uploaded from elsewhere and we dont incurr output cost from SNS
            # Regardless, we still need to increment the data transfer size
            if from_instance_node:
                from_region_id = from_instance_node.region_id
                self._manage_data_transfer_dict(
                    from_instance_node.tracked_data_output_sizes, from_region_id, sns_data_transfer_size
                )

                # Payload is uploaded to the SNS
                self._manage_sns_invocation_data_transfer_dict(
                    from_instance_node.sns_data_call_and_output_sizes, from_region_id, sns_data_transfer_size
                )

            # Data move from the predecessor node to the current node
            ## This means that the current node should have the data input size incremented
            self._manage_data_transfer_dict(
                to_instance_node.tracked_data_input_sizes, from_region_id, sns_data_transfer_size
            )

            return cumulative_runtime

        return 0.0

    def _handle_simulated_edge(
        self, current_edge: SimulatedInstanceEdge, edge_reached_time_to_sns_data: list[tuple[float, dict[str, Any]]]
    ) -> None:
        # Get the transmission information of the edge
        transmission_info = current_edge.get_simulated_transmission_information()
        starting_runtime = transmission_info["starting_runtime"]
        cumulative_runtime = transmission_info["cumulative_runtime"]
        sns_data_transfer_size = transmission_info["sns_data_transfer_size"]

        edge_reached_time_to_sns_data.append(
            (
                starting_runtime,
                {
                    "from_instance_node": current_edge.from_instance_node,
                    "to_instance_node": current_edge.to_instance_node,
                    "cumulative_runtime": cumulative_runtime,
                    "sns_data_transfer_size": sns_data_transfer_size,
                },
            )
        )

    def _handle_real_edge(
        self,
        current_edge: InstanceEdge,
        successor_is_sync_node: bool,
        sync_edge_upload_data: list[tuple[float, float]],
        edge_reached_time_to_sns_data: list[tuple[float, dict[str, Any]]],
    ) -> bool:
        node_invoked: bool = False

        # Materialize the edge
        transmission_info = current_edge.get_transmission_information(successor_is_sync_node)
        if transmission_info:
            if current_edge.conditionally_invoked:
                # For the normal execution case, we should get the size of data transfer, and the transfer latency.
                # If this node is a sync predecessor, we should also retrieve the sync_sizes_gb this denotes
                # the size of the sync table that need to be updated (invoked twice) to its successor, and also indicates
                # that the edge is a sync edge, and thus it transfer the data through the sync table rather than directly through SNS.
                starting_runtime = transmission_info["starting_runtime"]
                cumulative_runtime = transmission_info["cumulative_runtime"]
                sns_data_transfer_size = transmission_info["sns_data_transfer_size"]

                edge_reached_time_to_sns_data.append(
                    (
                        starting_runtime,
                        {
                            "from_instance_node": current_edge.from_instance_node,
                            "to_instance_node": current_edge.to_instance_node,
                            "cumulative_runtime": cumulative_runtime,
                            "sns_data_transfer_size": sns_data_transfer_size,
                        },
                    )
                )

                # Sync node must have more than 1 predecessor, all sync edges must have a predecessor
                # For sync nodes, payload is uploaded to the sync table and not through SNS
                if successor_is_sync_node:
                    if current_edge.from_instance_node == None:
                        raise ValueError(
                            f"Sync node must have a predecessor, destination instance: {current_edge.to_instance_node.instance_id}"
                        )
                    sync_info = transmission_info["sync_info"]
                    sync_size = sync_info["sync_size"]
                    consumed_wcu = sync_info["consumed_dynamodb_write_capacity_units"]
                    dynamodb_upload_size = sync_info["dynamodb_upload_size"]

                    sync_upload_auxiliary_info = sync_info["sync_upload_auxiliary_info"]
                    sync_edge_upload_data.append(sync_upload_auxiliary_info)

                    # Increment the consumed write capacity at the location
                    # of the SYNC Node (Outgoing node, Current node)
                    current_edge.to_instance_node.tracked_dynamodb_write_capacity += consumed_wcu

                    # Data denoted by dynamodb_upload_size is uploaded to the sync table (Move out of the predecessor node, and into the current, sync node)
                    self._manage_data_transfer_dict(
                        current_edge.from_instance_node.tracked_data_output_sizes,
                        current_edge.to_instance_node.region_id,
                        dynamodb_upload_size,
                    )
                    self._manage_data_transfer_dict(
                        current_edge.to_instance_node.tracked_data_input_sizes,
                        current_edge.from_instance_node.region_id,
                        dynamodb_upload_size,
                    )

                    # Data moves from the location of the sync node to the predecessor node
                    ## This means that the predecessor node should have the data input size incremented
                    ## This means that the current node should have the data output size incremented
                    self._manage_data_transfer_dict(
                        current_edge.to_instance_node.tracked_data_output_sizes,
                        current_edge.from_instance_node.region_id,
                        sync_size,
                    )
                    self._manage_data_transfer_dict(
                        current_edge.from_instance_node.tracked_data_input_sizes,
                        current_edge.to_instance_node.region_id,
                        sync_size,
                    )

                # Mark that the node was invoked
                node_invoked = True
            else:
                if current_edge.from_instance_node == None:
                    raise ValueError(
                        f"Sync node must have a predecessor, destination instance: {current_edge.to_instance_node.instance_id}"
                    )

                # For the non-execution case, we should get the instances that the sync node will write to
                # This will increment the consumed write capacity of the sync node, and also the data transfer size
                non_execution_infos = transmission_info["non_execution_info"]
                for non_execution_info in non_execution_infos:
                    simulated_predecessor_instance_id = non_execution_info["predecessor_instance_id"]
                    sync_node_instance_id = non_execution_info["sync_node_instance_id"]
                    consumed_wcu = non_execution_info["consumed_dynamodb_write_capacity_units"]
                    sync_data_response_size = non_execution_info["sync_size"]

                    # Get the sync node
                    ## Append the consumed write capacity to the sync node
                    sync_node = self._get_node(sync_node_instance_id)
                    sync_node.tracked_dynamodb_write_capacity += consumed_wcu

                    # Data of sync_data_response_size is moved from the sync node to the current node
                    self._manage_data_transfer_dict(
                        sync_node.tracked_data_output_sizes,
                        current_edge.to_instance_node.region_id,
                        sync_data_response_size,
                    )
                    self._manage_data_transfer_dict(
                        current_edge.to_instance_node.tracked_data_input_sizes,
                        sync_node.region_id,
                        sync_data_response_size,
                    )

                    # Add a simulated edge from the edge from node to the sync node
                    self._create_simulated_edge(
                        current_edge.from_instance_node.instance_id,
                        current_edge.to_instance_node.instance_id,
                        simulated_predecessor_instance_id,
                        sync_node_instance_id,
                    )
        else:
            real_node = current_edge.from_instance_node.invoked if current_edge.from_instance_node else False
            if real_node:
                # In the case the edge is real, there should never be a case where
                # there are no transmission information
                raise ValueError(
                    f"No transmission info in edge",
                    f"Node: {current_edge.to_instance_node.instance_id}",
                    f"Conditionally Invoked: {current_edge.conditionally_invoked}",
                    f"To: {current_edge.to_instance_node.instance_id}",
                )

        return node_invoked

    def calculate_overall_cost_runtime_carbon(self) -> dict[str, float]:
        cumulative_cost = 0.0
        max_runtime = 0.0
        cumulative_execution_carbon = 0.0
        cumulative_transmission_carbon = 0.0
        for node in self._nodes.values():
            node_carbon_cost_runtime = node.calculate_carbon_cost_runtime()
            cumulative_cost += node_carbon_cost_runtime["cost"]
            cumulative_execution_carbon += node_carbon_cost_runtime["execution_carbon"]
            cumulative_transmission_carbon += node_carbon_cost_runtime["transmission_carbon"]
            max_runtime = max(max_runtime, node_carbon_cost_runtime["runtime"])

        # print("\nFinal Results:")
        # print(f"Cost: {cumulative_cost}")
        # print(f"Runtime: {max_runtime}")
        # print(
        #     f"Carbon: EX- {cumulative_execution_carbon}, TR- {cumulative_transmission_carbon}, overall- {cumulative_execution_carbon + cumulative_transmission_carbon}"
        # )

        return {
            "cost": cumulative_cost,
            "runtime": max_runtime,
            "carbon": cumulative_execution_carbon + cumulative_transmission_carbon,
            "execution_carbon": cumulative_execution_carbon,
            "transmission_carbon": cumulative_transmission_carbon,
        }

    def _get_node(self, instance_index: int) -> InstanceNode:
        # Get node if exists, else create a new node
        if instance_index not in self._nodes:
            # Create new node
            node = InstanceNode(self._input_manager, instance_index)
            self._nodes[instance_index] = node

        return self._nodes[instance_index]

    def _create_edge(self, from_instance_index: int, to_instance_index: int) -> InstanceEdge:
        # Create a new edge
        from_instance_node: Optional[InstanceNode] = (
            self._get_node(from_instance_index) if from_instance_index != -1 else None
        )
        edge = InstanceEdge(self._input_manager, from_instance_node, self._get_node(to_instance_index))

        # Add the edge to the edge dictionary
        if to_instance_index not in self._edges:
            self._edges[to_instance_index] = {}
        self._edges[to_instance_index][from_instance_index] = edge

        return edge

    def _create_simulated_edge(
        self, from_instance_id: int, uninvoked_instance_id: int, simulated_sync_predecessor_id: int, sync_node_id: int
    ) -> SimulatedInstanceEdge:
        # Create a new simulated edge
        simulated_edge = SimulatedInstanceEdge(
            self._input_manager,
            self._get_node(from_instance_id),
            self._get_node(sync_node_id),
            uninvoked_instance_id,
            simulated_sync_predecessor_id,
        )

        # Add the edge to the simulated edges dictionary
        if sync_node_id not in self._simulated_edges:
            self._simulated_edges[sync_node_id] = {}
        self._simulated_edges[sync_node_id][from_instance_id] = simulated_edge

        # print(f"Created edge on {sync_node_id} from {from_instance_id} to {sync_node_id} (Uninvoked: {uninvoked_instance_id}, Sync Predecessor: {simulated_sync_predecessor_id})")

        return simulated_edge

    def _manage_data_transfer_dict(
        self, data_transfer_dict: dict[int, float], region_id: int, data_transfer_size: float
    ) -> None:
        if region_id not in data_transfer_dict:
            data_transfer_dict[region_id] = 0.0

        data_transfer_dict[region_id] += data_transfer_size

    def _manage_sns_invocation_data_transfer_dict(
        self, sns_data_transfer_dict: dict[str, list[float]], region_id: int, data_transfer_size: float
    ) -> None:
        if region_id not in sns_data_transfer_dict:
            sns_data_transfer_dict[region_id] = []
        sns_data_transfer_dict[region_id].append(data_transfer_size)

    def _get_predecessor_edges(
        self, instance_index: int, simulated_edges: bool
    ) -> list[Union[InstanceEdge, SimulatedInstanceEdge]]:
        edge_dict = self._edges
        if simulated_edges:
            edge_dict = self._simulated_edges

        return list(edge_dict.get(instance_index, {}).values())
