import numpy as np

from multi_x_serverless.routing.solver.solver import Solver


class StochasticGradientDescentSolver(Solver):
    def _solve(self, regions: np.ndarray) -> list[tuple[dict, float, float, float]]:
        # TODO (#14): Implement this function
        return np.zeros((len(regions), len(self._workflow_config.functions)))
