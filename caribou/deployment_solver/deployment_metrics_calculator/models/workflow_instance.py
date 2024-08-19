import random
from typing import Any, Optional

from caribou.deployment_solver.deployment_input.input_manager import InputManager
from caribou.deployment_solver.deployment_metrics_calculator.models.instance_edge import InstanceEdge
from caribou.deployment_solver.deployment_metrics_calculator.models.instance_node import InstanceNode
from caribou.deployment_solver.deployment_metrics_calculator.models.simulated_instance_edge import SimulatedInstanceEdge


class WorkflowInstance:  # pylint: disable=too-many-instance-attributes
    def __init__(
        self,
        input_manager: InputManager,
        instance_deployment_regions: list[int],
        start_hop_instance_index: int,
        consider_from_client_latency: bool,
    ) -> None:
        self._input_manager: InputManager = input_manager
        self._consider_from_client_latency: bool = consider_from_client_latency
        self._start_hop_instance_id: int = start_hop_instance_index

        # This is the flag that determines if the WPD is retrieved at the function or not.
        # This is an important flag as it determines if the start hop is a possible
        # redirector function or not. Start hop will be a redirector function in the case
        # where the region at the start hop is not the home region, and the WPD is
        # retrieved at the function.
        self._has_retrieved_wpd_at_function: bool = self._retrieved_wpd_at_function()

        # If wpd is retrieved in the function AND that the start hop is not at the home region,
        # then we will also have a redirector function.
        self._redirector_exists: bool = (
            self._has_retrieved_wpd_at_function
            and not instance_deployment_regions[self._start_hop_instance_id]
            == self._input_manager.get_home_region_index()
        )

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
        virtual_client_node: InstanceNode
        if self._redirector_exists:
            # Create the virtual client node
            virtual_client_node = self._get_node(-2)
            virtual_client_node.region_id = -1  # From unknown region

            # Create the redirector function node, which is present in the home region.
            redirector_node = self._get_node(-1)
            redirector_node.region_id = self._input_manager.get_home_region_index()
            redirector_node.actual_instance_id = self._start_hop_instance_id
        else:
            # Create the virtual client node
            virtual_client_node = self._get_node(-1)
            virtual_client_node.region_id = -1

        # Configure the regions of the other nodes
        for instance_index, region_index in enumerate(instance_deployment_regions):
            current_node = self._get_node(instance_index)
            current_node.region_id = region_index

    def add_start_hop(self, starting_instance_index: int) -> None:
        wpd_retrieval_node: InstanceNode
        virtual_client_node: InstanceNode
        redirector_node: Optional[InstanceNode] = None
        start_hop_node: InstanceNode = self._get_node(starting_instance_index)
        if not self._has_retrieved_wpd_at_function:
            # If wpd is not retrieved at the function, then it will always be retrieved
            # in the virtual start hop. (Indicating client invocation region)
            # Also we will NEVER have a redirector as the request will always be sent
            # to the desired region.
            wpd_retrieval_node = self._get_node(-1)
            virtual_client_node = wpd_retrieval_node
        else:
            if self._redirector_exists:
                # If the redirector exists, and retrieve WPD then we indicate
                # that the redirector function is invoked.
                wpd_retrieval_node = self._get_node(-1)
                redirector_node = wpd_retrieval_node
                virtual_client_node = self._get_node(-2)
            else:
                # Redirector does not exist, wpd is pulled at the start hop instance
                wpd_retrieval_node = start_hop_node

                # There also exists a virtual client node
                virtual_client_node = self._get_node(-1)

        # Set node invoked flags (Can be either virtual or real)
        # Note: redirector_node will always be wpd_retrieval_node if it exists
        wpd_retrieval_node.invoked = True
        virtual_client_node.invoked = True
        start_hop_node.invoked = True

        # Create an new edge that goes from virtual client to the start hop
        # (or redirector if it exists)
        virtual_client_to_first_node_edge: InstanceEdge
        if redirector_node is not None:
            virtual_client_to_first_node_edge = self._create_edge(
                virtual_client_node.nominal_instance_id, redirector_node.nominal_instance_id
            )
        else:
            virtual_client_to_first_node_edge = self._create_edge(
                virtual_client_node.nominal_instance_id, start_hop_node.nominal_instance_id
            )

        # It is impossible for the virtual client edge NOT to be invoked
        virtual_client_to_first_node_edge.conditionally_invoked = True

        # Get the WPD size of the starting node (This is the data that is downloaded from
        # system region to the wpd retrieval node)
        ## While data is also moved FROM system region, we do not need to track or model
        ## this data transfer as it is not part of the workflow

        start_hop_node_info = self._input_manager.get_start_hop_info()
        dynamodb_read_capacity_units = start_hop_node_info["read_capacity_units"]
        wpd_retrieval_node.tracked_dynamodb_read_capacity += dynamodb_read_capacity_units

        ## Deal with the download size of the starting node
        workflow_placement_decision_size = start_hop_node_info["workflow_placement_decision_size"]

        ## Data is downloaded from the system region to the wpd_retrieval_node node
        ## We define -2 as the system region location.
        self._manage_data_transfer_dict(
            wpd_retrieval_node.tracked_data_input_sizes,
            -2,
            workflow_placement_decision_size,
        )

        # If the redirector exists, we need to handle the edge and node of it
        if redirector_node is not None:
            # Handle edge
            self.add_edge(redirector_node.nominal_instance_id, start_hop_node.nominal_instance_id, True)

            # handle/materialize the node)
            self.add_node(redirector_node.nominal_instance_id)

    def add_edge(self, from_instance_index: int, to_instance_index: int, invoked: bool) -> None:
        # Get the from nod, We need this to determine if the edge is actually
        # invoked or not, as if the from node is not invoked
        # then the edge is not invoked.
        from_node: InstanceNode = self._get_node(from_instance_index)

        # Get the edge
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
        # Get the current node
        nominal_instance_id: int = instance_index
        current_node = self._get_node(instance_index)
        actual_instance_id: int = current_node.actual_instance_id

        # Look through all the real edges linking to this node
        node_invoked: bool = False
        real_predecessor_edges: list[InstanceEdge] = self._get_predecessor_edges(nominal_instance_id, False)
        
        is_sync_node: bool = len(real_predecessor_edges) > 1
        sync_edge_upload_data: list[tuple[float, float]] = []
        edge_reached_time_to_sns_data: list[tuple[float, dict[str, Any]]] = []
        for current_edge in real_predecessor_edges:
            # Real edge are defined as edges that are actually apart of the workflow
            # This will also manage and determine if this current node actually is invoked
            # and also create virtual edges for the purposes of handling non-execution.
            current_node_invoked = self._handle_real_edge(
                current_edge, is_sync_node, sync_edge_upload_data, edge_reached_time_to_sns_data
            )
            node_invoked = node_invoked or current_node_invoked

        # Handle sync upload auxiliary data
        # This the write capacity unit of the sync node
        # Purely from data upload and download, where upload
        # is done via UpdateItem (Which consumes whole write
        # capacity unit of size of the table)
        if len(sync_edge_upload_data) > 0:
            capacity_units = self._input_manager.calculate_dynamodb_capacity_unit_of_sync_edges(sync_edge_upload_data)
            current_node.tracked_dynamodb_write_capacity += capacity_units["write_capacity_units"]
            current_node.tracked_dynamodb_read_capacity += capacity_units["read_capacity_units"]

        # Calculate the cumulative runtime of the node
        # Only if the node was invoked
        if node_invoked:
            # We only care about simulated edges IFF the node was invoked
            # As it determines the actual runtime of the node (and represent SNS call)
            simulated_predecessor_edges: list[SimulatedInstanceEdge] = self._get_predecessor_edges(
                nominal_instance_id, True
            )
            for simulated_edge in simulated_predecessor_edges:
                self._handle_simulated_edge(simulated_edge, edge_reached_time_to_sns_data)

            # Calculate the cumulative runtime of the node and the data transfer during execution
            cumulative_runtime = self._handle_sns_invocation(edge_reached_time_to_sns_data)

            # Calculate and set the cumulative runtime of the node
            # and the data transfer during execution
            (
                current_node.cumulative_runtimes,
                current_node.execution_time,
                data_transfer_during_execution,
                # ) = self._input_manager.get_node_runtimes_and_data_transfer(
                #     instance_index, current_node.region_id, cumulative_runtime
                # )
            ) = self._input_manager.get_node_runtimes_and_data_transfer(
                actual_instance_id, current_node.region_id, cumulative_runtime, current_node.is_redirector
            )
            # This data transfer during executionr refers the data that our wrapper
            # does not account or send out. This mainly from user code or
            # from other lambda background processes.
            current_node.data_transfer_during_execution += data_transfer_during_execution

        # Set the node invoked flag
        current_node.invoked = node_invoked

        return node_invoked

    def _handle_sns_invocation(self, edge_reached_time_to_sns_data: list[tuple[float, dict[str, Any]]]) -> float:
        # Sort the sns data by the starting runtime, aka when it
        # was invoked, we only want the one with the longest runtime
        # As that is the one that will actually cause the SNS invocation
        edge_reached_time_to_sns_data.sort(key=lambda x: x[0], reverse=True)

        if len(edge_reached_time_to_sns_data) > 0:
            # Get the edge that will invoke the SNS
            sns_edge_data = edge_reached_time_to_sns_data[0][1]
            cumulative_runtime: float = sns_edge_data["cumulative_runtime"]
            sns_data_transfer_size: float = sns_edge_data["sns_data_transfer_size"]
            from_instance_node: InstanceNode = sns_edge_data["from_instance_node"]
            to_instance_node: InstanceNode = sns_edge_data["to_instance_node"]

            from_region_id = from_instance_node.region_id
            to_region_id = to_instance_node.region_id

            # Data moves OUT from the predecessor node
            self._manage_data_transfer_dict(
                from_instance_node.tracked_data_output_sizes, to_region_id, sns_data_transfer_size
            )

            # Data move from the predecessor node to the current node
            self._manage_data_transfer_dict(
                to_instance_node.tracked_data_input_sizes, from_region_id, sns_data_transfer_size
            )

            # Payload is uploaded to the SNS
            # Aka the previouse node performs a SNS invocation
            self._manage_sns_invocation_data_transfer_dict(
                from_instance_node.sns_data_call_and_output_sizes, to_region_id, sns_data_transfer_size
            )

            return cumulative_runtime

        return 0.0

    def _handle_simulated_edge(
        self, current_edge: SimulatedInstanceEdge, edge_reached_time_to_sns_data: list[tuple[float, dict[str, Any]]]
    ) -> None:
        # Get the transmission information of the edge
        transmission_info = current_edge.get_simulated_transmission_information()
        if not transmission_info:
            raise ValueError(
                "No transmission info in edge",
                f"Node: {current_edge.to_instance_node.nominal_instance_id}",
                f"To: {current_edge.to_instance_node.nominal_instance_id}",
            )
        starting_runtime = transmission_info["starting_runtime"]
        cumulative_runtime = transmission_info["cumulative_runtime"]
        sns_data_transfer_size = transmission_info["sns_data_transfer_size"]

        # Simulated edge only have the potential of performing a SNS invocation
        # This is handled by inscribing this information in the edge_reached_time_to_sns_data
        # to be handle along with real edge reached time to SNS data.
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
        transmission_info = current_edge.get_transmission_information(
            successor_is_sync_node, self._consider_from_client_latency
        )
        if transmission_info:
            if current_edge.conditionally_invoked:
                # For the normal execution case, we should get the size of data transfer, and the transfer latency.
                # If this node is a sync predecessor, we should also retrieve the sync_sizes_gb this denotes
                # the size of the sync table that need to be updated (invoked twice) to its successor,
                # and also indicates that the edge is a sync edge, and thus it transfer the data through
                # the sync table rather than directly through SNS.
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

                # If the successor is a sync node, there are additional data transfer
                # that occurs, if not then the data transfer is only through SNS.
                if successor_is_sync_node:
                    # Sync node must have more than 1 predecessor, all sync edges must have a predecessor
                    # For sync nodes, payload is uploaded to the sync table and not through SNS
                    if current_edge.from_instance_node.nominal_instance_id == -1:
                        raise ValueError(
                            "Sync node must have a predecessor that is not the start hop!"
                            f"instance: {current_edge.to_instance_node.nominal_instance_id}"
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

                    # Data denoted by dynamodb_upload_size is uploaded to the sync
                    # table (Move out of the predecessor node, and into the current, sync node)
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
                # Non-invoked node cannot be a virtual start node.
                if current_edge.from_instance_node.nominal_instance_id == -1:
                    raise ValueError(
                        "Non-execution node must have a predecessor that is not the start hop!"
                        f"instance: {current_edge.to_instance_node.nominal_instance_id}"
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
                    # Where current node refers to the node calling the non-execution edge
                    self._manage_data_transfer_dict(
                        sync_node.tracked_data_output_sizes,
                        current_edge.from_instance_node.region_id,
                        sync_data_response_size,
                    )
                    self._manage_data_transfer_dict(
                        current_edge.from_instance_node.tracked_data_input_sizes,
                        sync_node.region_id,
                        sync_data_response_size,
                    )

                    # Add a simulated edge from the edge from node to the sync node
                    self._create_simulated_edge(
                        current_edge.from_instance_node.nominal_instance_id,
                        current_edge.to_instance_node.nominal_instance_id,
                        simulated_predecessor_instance_id,
                        sync_node_instance_id,
                    )
        else:
            real_node = current_edge.from_instance_node.invoked
            if real_node:
                # In the case the edge is real, there should never be a case where
                # there are no transmission information
                raise ValueError(
                    "No transmission info in edge",
                    f"Node: {current_edge.to_instance_node.nominal_instance_id}",
                    f"Conditionally Invoked: {current_edge.conditionally_invoked}",
                    f"To: {current_edge.to_instance_node.nominal_instance_id}",
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
        # Create a new edge (And specify the from and to nodes)
        edge = InstanceEdge(self._input_manager, self._get_node(from_instance_index), self._get_node(to_instance_index))

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

        return simulated_edge

    def _manage_data_transfer_dict(
        self, data_transfer_dict: dict[int, float], region_id: int, data_transfer_size: float
    ) -> None:
        if region_id not in data_transfer_dict:
            data_transfer_dict[region_id] = 0.0

        data_transfer_dict[region_id] += data_transfer_size

    def _manage_sns_invocation_data_transfer_dict(
        self, sns_data_transfer_dict: dict[int, list[float]], region_id: int, data_transfer_size: float
    ) -> None:
        if region_id not in sns_data_transfer_dict:
            sns_data_transfer_dict[region_id] = []
        sns_data_transfer_dict[region_id].append(data_transfer_size)

    def _get_predecessor_edges(self, instance_index: int, simulated_edges: bool) -> list[Any]:
        edge_dict: dict[int, Any] = self._edges
        if simulated_edges:
            edge_dict = self._simulated_edges

        return list(edge_dict.get(instance_index, {}).values())

    def _retrieved_wpd_at_function(self) -> bool:
        retrieved_wpd_at_function_probability = self._input_manager.get_start_hop_retrieve_wpd_probability()
        return random.random() < retrieved_wpd_at_function_probability
