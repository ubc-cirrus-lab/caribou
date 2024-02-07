from typing import Any

from multi_x_serverless.routing.solver.input.components.calculator import InputCalculator

from multi_x_serverless.routing.solver.input.components.loaders.carbon_loader import CarbonLoader

from multi_x_serverless.routing.solver.input.components.calculators.cost_calculator import CostCalculator

class CarbonCalculator(InputCalculator):
    def __init__(self, carbon_loader: CarbonLoader, cost_calculator: CostCalculator) -> None:
        super().__init__()
        self._carbon_loader: CarbonLoader = carbon_loader
        self._cost_calculator: CostCalculator = cost_calculator

    def calculate_execution_carbon(self, instance_name: str, region_name: str, consider_probabilistic_invocations: bool = False) -> float:
        if consider_probabilistic_invocations:
            # TODO (#76): Implement probabilistic invocations
            raise NotImplementedError("Probabilistic invocations are not supported yet")
        else:
            # return self._calculate_raw_runtime(instance_name, region_name, False)
            pass

    def calculate_transmission_carbon(
        self, 
        from_instance_name: str,
        to_instance_name: str,
        from_region_name: str,
        to_region_name: str,
        consider_probabilistic_invocations: bool = False) -> float:
        if consider_probabilistic_invocations:
            # TODO (#76): Implement probabilistic invocations
            raise NotImplementedError("Probabilistic invocations are not supported yet")