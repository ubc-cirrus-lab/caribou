import numpy as np


class Formatter:
    def __init__(self) -> None:
        pass

    def format(self, results: list[tuple[dict, float, float, float]]) -> list[dict]:
        # TODO (33): Implement output formatter for solver
        return [self._format_result(result) for result in results]  # Format result to a desirable way

    def _format_result(self, result: tuple[dict, float, float, float]) -> dict:
        solution, cost, runtime, carbon = result
        return {
            "solution": solution,
            "cost": cost,
            "runtime": runtime,
            "carbon": carbon,
        }
