from typing import Any, Optional

from caribou.deployment_solver.deployment_input.input_manager import InputManager
from caribou.deployment_solver.deployment_metrics_calculator.models.instance_node import InstanceNode

class SimulatedInstanceEdge:
    def __init__(self,
                input_manager: InputManager,
                from_instance_node: InstanceNode,
                to_instance_node: InstanceNode,
                uninvoked_instance_id: int,
                simulated_sync_predecessor_id: int,
                ) -> None:
        self._input_manager: InputManager = input_manager

        # Edge always goes to an instance 
        self.from_instance_node: InstanceNode = from_instance_node

        # To instance is also the sync_node
        self.to_instance_node: InstanceNode = to_instance_node
        
        self.uninvoked_instance_id: int = uninvoked_instance_id
        self.simulated_sync_predecessor_instance_id: int = simulated_sync_predecessor_id

    def get_simulated_transmission_information(self) -> Optional[dict[str, Any]]:
        # Check the edge if it is a simulated edge
        # If the edge is simulated, then we should return the transmission information
        # If the edge is NOT simulated, then we should return None
        if (self.from_instance_node and not self.from_instance_node.invoked) or not self.to_instance_node:
            return None

        from_instance_id = self.from_instance_node.instance_id
        uninvoked_instance_id = self.uninvoked_instance_id
        simulated_sync_predecessor_id = self.simulated_sync_predecessor_instance_id
        sync_node_id = self.to_instance_node.instance_id

        from_region_id = self.from_instance_node.region_id
        to_region_id = self.to_instance_node.region_id

        cumulative_runtime = 0.0
        # For non-starting edges, we should add the cumulative runtime of the parent node
        if self.from_instance_node is not None:
            # The time to call successor node is actually the cumulative time of the
            # parent node calling the uninvoked node
            cumulative_runtime += self.from_instance_node.get_cumulative_runtime(uninvoked_instance_id)

        # Those edges are not apart of the workflow
        # and are only used to handle latencies of non-execution of ancestor nodes
        transmission_info = self._input_manager.get_simulated_transmission_info(from_instance_id, uninvoked_instance_id, simulated_sync_predecessor_id, sync_node_id, from_region_id, to_region_id, cumulative_runtime)

        return transmission_info