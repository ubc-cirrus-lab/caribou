from multi_x_serverless.routing.current.solver.solver import Solver

import numpy as np


class SimpleSolver(Solver):
    def solve(self, regions: np.ndarray, functions: np.ndarray) -> list[tuple[dict, float, float, float]]:
        # TODO (#19): Implement simple solver
        return []
