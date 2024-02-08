from typing import Optional

from multi_x_serverless.routing.solver.objective_function.objective_function import ObjectiveFunction


class AnyImprovementObjectiveFunction(ObjectiveFunction):
    @staticmethod
    def calculate(cost: float, runtime: float, carbon: float, **kwargs: Optional[float]) -> bool:
        best_cost = kwargs.get("best_cost", float("inf"))
        best_runtime = kwargs.get("best_runtime", float("inf"))
        best_carbon = kwargs.get("best_carbon", float("inf"))

        if best_carbon is None:
            best_carbon = float("inf")
        if best_runtime is None:
            best_runtime = float("inf")
        if best_cost is None:
            best_cost = float("inf")

        return any(
            [
                cost < best_cost,
                runtime < best_runtime,
                carbon < best_carbon,
            ]
        )
