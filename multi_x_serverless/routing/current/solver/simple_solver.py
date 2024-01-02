import numpy as np

from multi_x_serverless.routing.current.solver.solver import Solver


class SimpleSolver(Solver):
    def _solve(self, regions: np.ndarray) -> list[tuple[dict, float, float, float]]:
        # TODO (#19): Implement simple solver
        return []
