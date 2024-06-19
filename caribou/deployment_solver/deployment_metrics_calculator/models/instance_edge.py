from typing import Any, Optional

from caribou.deployment_solver.deployment_input.input_manager import InputManager
from caribou.deployment_solver.deployment_metrics_calculator.models.instance_node import InstanceNode

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
            cumulative_runtime = 0.0
            # For non-starting edges, we should add the cumulative runtime of the parent node
            if self.from_instance_node is not None:
                cumulative_runtime += self.from_instance_node.get_cumulative_runtime(to_instance_id)

            transmission_info = self._input_manager.get_transmission_info(from_instance_id, to_instance_id, from_region_id, to_region_id, cumulative_runtime, self.contain_alternative_latency)
        else:
            # This is the case where the edge is conditionally NOT invoked, and thus
            # will need to look at the non-execution information.
            transmission_info = self._input_manager.get_non_execution_info(from_instance_id, to_instance_id, from_region_id, to_region_id)

        return transmission_info