from typing import Any

from caribou.deployment_solver.deployment_input.input_manager import InputManager


# pylint: disable=too-many-instance-attributes
class InstanceNode:
    def __init__(self, input_manager: InputManager, instane_id: int) -> None:
        self._input_manager: InputManager = input_manager

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
        self.sns_data_call_and_output_sizes: dict[int, list[float]] = {}

        # Store the cumulative dynamodb read and
        # write capacity of the node
        self.tracked_dynamodb_read_capacity: float = 0.0
        self.tracked_dynamodb_write_capacity: float = 0.0

        # Store the cumulative runtime of the node
        # The key is the instance index of the successor
        # The value is the cumulative runtime of when this
        # node invokes the successor
        self.cumulative_runtimes: dict[str, Any] = {"current": 0.0, "successors": {}}

        # Store the node execution time
        self.execution_time: float = 0.0

    def get_cumulative_runtime(self, successor_instance_index: int) -> float:
        # Get the cumulative runtime of the successor edge
        # If there are no specifiec runtime for the successor
        # then return the current runtime of the node (Worse case scenario)
        return self.cumulative_runtimes["successors"].get(successor_instance_index, self.cumulative_runtimes["current"])

    def calculate_carbon_cost_runtime(self) -> dict[str, float]:
        # Calculate the cost and carbon of the node
        # based on the input/output size and dynamodb
        # read/write capacity

        # TODO: If instance ID == -1, this is a virtual start node
        if self.instance_id == -1:
            # print(f"Calculate Virtual Node (Virtual Start Node {self.instance_id}):")
            calculated_metrics = self._input_manager.calculate_cost_and_carbon_virtual_start_instance(
                self.tracked_data_input_sizes,
                self.tracked_data_output_sizes,
                self.sns_data_call_and_output_sizes,
                self.tracked_dynamodb_read_capacity,
                self.tracked_dynamodb_write_capacity,
            )
        else:
            # print(f"Calculate Real Node, {self.instance_id} -> {self.invoked}:")
            calculated_metrics = self._input_manager.calculate_cost_and_carbon_of_instance(
                self.execution_time,
                self.instance_id,
                self.region_id,
                self.tracked_data_input_sizes,
                self.tracked_data_output_sizes,
                self.sns_data_call_and_output_sizes,
                self.data_transfer_during_execution,
                self.tracked_dynamodb_read_capacity,
                self.tracked_dynamodb_write_capacity,
                self.invoked,
            )

        # We only care about the runtime if the node was invoked
        runtime = self.cumulative_runtimes["current"] if self.invoked else 0.0
        return {
            "cost": calculated_metrics["cost"],
            "runtime": runtime,
            "execution_carbon": calculated_metrics["execution_carbon"],
            "transmission_carbon": calculated_metrics["transmission_carbon"],
        }
