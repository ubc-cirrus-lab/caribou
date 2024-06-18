class InstanceEdge:
    def __init__(self, input_manager: InputManager) -> None:
        self._input_manager: InputManager = input_manager
        self._max_runtime: float = 0.0
        self._nodes: dict[int, dict[str, int]] = {}
    
    def add_start_hop(self, starting_instance_index: int) -> None:
        self._nodes[starting_instance_index] = {
            "region_index": -1,
            "runtime": 0,
        }

    def add_node(self, instance_index: int, region_index: int) -> None:
        pass
    
    def add_edge(self, from_instance_index: int, to_instance_index: int, invoked: bool) -> None:
        pass

    def calculate_overall_cost_runtime_carbon(self, deployment: list[int]) -> dict[str, float]:
        # Get average and tail cost/carbon/runtime from Monte Carlo simulation
        # return self._perform_monte_carlo_simulation(deployment)
        cost = 0.0
        runtime = 0.0
        carbon = 0.0
        return {
            "cost": cost,
            "runtime": self._max_runtime,
            "carbon": carbon,
        }
