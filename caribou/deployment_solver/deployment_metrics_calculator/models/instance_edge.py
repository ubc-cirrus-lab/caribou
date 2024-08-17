from typing import Any, Optional

from caribou.deployment_solver.deployment_input.input_manager import InputManager
from caribou.deployment_solver.deployment_metrics_calculator.models.instance_node import InstanceNode


class InstanceEdge:
    def __init__(
        self, input_manager: InputManager, from_instance_node: InstanceNode, to_instance_node: InstanceNode
    ) -> None:
        self._input_manager: InputManager = input_manager

        # Edge always goes to an instance, (Or a virtual instance if it is a start node
        # This type of instances has instance ID of -1)
        self.from_instance_node: InstanceNode = from_instance_node
        self.to_instance_node: InstanceNode = to_instance_node

        # Whether the edge is
        # invoked conditionally.
        self.conditionally_invoked: bool = False

    def get_transmission_information(
        self, successor_is_sync_node: bool, consider_from_client_latency: bool
    ) -> Optional[dict[str, Any]]:
        # Check the edge if it is a real edge
        # First get the parent node
        # If the parent node is invoked, then the edge is real
        # If the parent node is not invoked, then the edge is not real
        # If the edge is NOT real, then we should return None
        if self.from_instance_node and not self.from_instance_node.invoked:
            return None

        # from_instance_id = self.from_instance_node.nominal_instance_id
        # to_instance_id = self.to_instance_node.nominal_instance_id
        from_instance_id = self.from_instance_node.actual_instance_id
        to_instance_id = self.to_instance_node.actual_instance_id
        from_region_id = self.from_instance_node.region_id
        to_region_id = self.to_instance_node.region_id

        # Those edges are actually apart of the workflow
        if self.conditionally_invoked:
            # This is the case where the edge is invoked conditionally
            cumulative_runtime = 0.0
            # For non-starting edges, we should add the cumulative runtime of the parent node
            if self.from_instance_node is not None:
                cumulative_runtime += self.from_instance_node.get_cumulative_runtime(to_instance_id)

            transmission_info = self._input_manager.get_transmission_info(
                from_instance_id,
                from_region_id,
                to_instance_id,
                to_region_id,
                cumulative_runtime,
                successor_is_sync_node,
                consider_from_client_latency,
            )
        else:
            # This is the case where the edge is conditionally NOT invoked, and thus
            # will need to look at the non-execution information.
            transmission_info = self._input_manager.get_non_execution_info(from_instance_id, to_instance_id)

        return transmission_info
