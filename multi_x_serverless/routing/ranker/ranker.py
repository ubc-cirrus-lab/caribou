from typing import Optional

from multi_x_serverless.routing.workflow_config import WorkflowConfig


class Ranker:
    def __init__(self, config: WorkflowConfig, home_deployment_metrics: Optional[dict[str, float]] = None) -> None:
        self._config = config
        self._home_deployment_metrics = home_deployment_metrics

        self.number_one_priority = self._get_number_one_priority()

    def _get_number_one_priority(self) -> str:
        if self._config.constraints is None or "priority_order" not in self._config.constraints:
            return "average_carbon"
        priority_order = self._config.constraints["priority_order"]
        return f"average_{priority_order[0]}"

    def rank(self, results: list[tuple[list[int], dict[str, float]]]) -> list[tuple[list[int], dict[str, float]]]:
        constraints = self._config.constraints

        if constraints is not None and "soft_resource_constraints" in constraints:
            soft_resource_constraints = constraints["soft_resource_constraints"]
            return self._rank_with_soft_resource_constraints(results, soft_resource_constraints)
        return self._rank_with_priority_order(results)

    def _rank_with_priority_order(
        self, results: list[tuple[list[int], dict[str, float]]]
    ) -> list[tuple[list[int], dict[str, float]]]:
        if self._config.constraints is None or "priority_order" not in self._config.constraints:
            return sorted(
                results, key=lambda x: (x[1]["average_carbon"], x[1]["average_runtime"], x[1]["average_cost"])
            )
        priority_order = self._config.constraints["priority_order"]
        sorted_order = sorted(results, key=lambda x: tuple(x[1][f"average_{i}"] for i in priority_order))
        return sorted_order

    def _rank_with_soft_resource_constraints(
        self, results: list[tuple[list[int], dict[str, float]]], soft_resource_constraints: dict
    ) -> list[tuple[list[int], dict[str, float]]]:
        ranked_results = []
        violations_to_results: dict[int, list[tuple[list[int], dict[str, float]]]] = {}
        for result in results:
            _, metrics = result
            number_of_violated_constraints = self._get_number_of_violated_constraints(
                soft_resource_constraints,
                metrics["average_cost"],
                metrics["average_runtime"],
                metrics["average_carbon"],
            )
            if number_of_violated_constraints not in violations_to_results:
                violations_to_results[number_of_violated_constraints] = []
            violations_to_results[number_of_violated_constraints].append(result)

        for number_of_violated_constraints in sorted(violations_to_results.keys()):
            ranked_results.extend(self._rank_with_priority_order(violations_to_results[number_of_violated_constraints]))
        return ranked_results

    def _get_number_of_violated_constraints(
        self, soft_resource_constraints: dict, cost: float, runtime: float, carbon: float
    ) -> int:
        number_of_violated_constraints = 0
        for constraint in soft_resource_constraints:
            if soft_resource_constraints[constraint]:
                if self._home_deployment_metrics:
                    home_deployment_metrics: dict[str, float] = self._home_deployment_metrics

                    if constraint == "cost":
                        if self.is_absolute_or_relative_failed(
                            cost, soft_resource_constraints[constraint], home_deployment_metrics["average_cost"]
                        ):
                            number_of_violated_constraints += 1
                    elif constraint == "runtime":
                        if self.is_absolute_or_relative_failed(
                            runtime, soft_resource_constraints[constraint], home_deployment_metrics["average_runtime"]
                        ):
                            number_of_violated_constraints += 1
                    elif constraint == "carbon":
                        if self.is_absolute_or_relative_failed(
                            carbon, soft_resource_constraints[constraint], home_deployment_metrics["average_carbon"]
                        ):
                            number_of_violated_constraints += 1
        return number_of_violated_constraints

    def is_absolute_or_relative_failed(self, value: float, constraint: dict, relative_to: float) -> bool:
        if not constraint or "value" not in constraint:
            return False
        if constraint["type"] == "absolute":
            return value > constraint["value"]
        if constraint["type"] == "relative":
            return value > constraint["value"] * relative_to
        return False
