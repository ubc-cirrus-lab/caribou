from typing import Any, Optional

from caribou.deployment_solver.deployment_input.input_manager import InputManager
# from caribou.deployment_solver.deployment_metrics_calculator.models.instance_edge import InstanceEdge
# from caribou.deployment_solver.deployment_metrics_calculator.models.instance_node import InstanceNode

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
        print('__init__')
        print(self.cumulative_runtimes)

    def get_cumulative_runtime(self, successor_instance_index: int) -> float:
        # Get the cumulative runtime of the successor edge
        # If there are no specifiec runtime for the successor
        # then return the current runtime of the node (Worse case scenario)
        print('get_cumulative_runtime')
        print(self.cumulative_runtimes)
        return self.cumulative_runtimes["sucessors"].get(successor_instance_index, self.cumulative_runtimes["current"])

    def calculate_carbon_cost_runtime(self) -> dict[str, float]:
        # Calculate the cost and carbon of the node
        # based on the input/output size and dynamodb
        # read/write capacity
        # print(f'Calculating cost and carbon for node: {self.instance_id}')
        # print(f"cumulative_runtimes: {self.cumulative_runtimes}")
        
        print('calculate_carbon_cost_runtime')
        print(self.cumulative_runtimes)
    
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
        # print(calculated_metrics)

        return {
            "cost": calculated_metrics['cost'],
            "carbon": calculated_metrics['carbon'],
            "runtime": self.cumulative_runtimes['current'],
        }