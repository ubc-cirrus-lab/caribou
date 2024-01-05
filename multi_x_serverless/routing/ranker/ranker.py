import numpy as np

from multi_x_serverless.routing.workflow_config import WorkflowConfig


class Ranker:
    def __init__(self, config: WorkflowConfig):
        self._config = config
        self._priority_order_name_to_index = {
            "cost": 0,
            "runtime": 1,
            "carbon": 2,
        }

    def rank(self, results: list[tuple[dict, float, float, float]]) -> list[tuple[dict, float, float, float]]:
        constraints = self._config.constraints

        if constraints is not None and "soft_resource_constraints" in constraints:
            soft_resource_constraints = constraints["soft_resource_constraints"]
            return self._rank_with_soft_resource_constraints(results, soft_resource_constraints)
        return self._rank_with_priority_order(results)

    def _rank_with_priority_order(
        self, results: list[tuple[dict, float, float, float]]
    ) -> list[tuple[dict, float, float, float]]:
        if self._config.constraints is None or "priority_order" not in self._config.constraints:
            return sorted(results, key=lambda x: (x[1], x[2], x[3]))
        else:
            priority_order = self._config.constraints["priority_order"]
            ordering = tuple(self._priority_order_name_to_index[metric] + 1 for metric in priority_order)
            sorted_order = sorted(results, key=lambda x: tuple(x[i] for i in ordering))
            return sorted_order

    def _rank_with_soft_resource_constraints(
        self, results: list[tuple[dict, float, float, float]], soft_resource_constraints: dict
    ) -> list[tuple[dict, float, float, float]]:
        ranked_results = []
        violations_to_results: dict[int, list[tuple[dict, float, float, float]]] = {}
        for result in results:
            _, cost, runtime, carbon = result
            number_of_violated_constraints = self._get_number_of_violated_constraints(
                soft_resource_constraints, cost, runtime, carbon
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
            if constraint == "cost":
                if cost > soft_resource_constraints[constraint]["value"]:
                    number_of_violated_constraints += 1
            elif constraint == "runtime":
                if runtime > soft_resource_constraints[constraint]["value"]:
                    number_of_violated_constraints += 1
            elif constraint == "carbon":
                if carbon > soft_resource_constraints[constraint]["value"]:
                    number_of_violated_constraints += 1
        return number_of_violated_constraints
