import numpy as np

from multi_x_serverless.routing.solver.solver import Solver


class SimpleSolver(Solver):
    def _solve(self, regions: list[dict]) -> list[tuple[dict, float, float, float]]:
        carbon = np.zeros(len(regions))
        cost = np.zeros(len(regions))
        runtime = np.zeros(len(regions))
        number_of_transmissions = self._dag.number_of_edges

        for data_source_name in self._data_sources:
            # Execution cost is the sum of the execution costs of all functions in the same region for the simple solver
            execution_matrix = self._data_sources[data_source_name].get_execution_matrix()

            row_sums = np.sum(execution_matrix, axis=1)

            if data_source_name == "cost":
                cost = row_sums
            elif data_source_name == "runtime":
                runtime = row_sums
            elif data_source_name == "carbon":
                carbon = row_sums

            # Transmission cost is the diagonal of the transmission matrix times the
            # number of transmissions between the functions
            transmission_matrix = self._data_sources[data_source_name].get_transmission_matrix()
            transmission_cost = np.diagonal(transmission_matrix) * number_of_transmissions

            if data_source_name == "cost":
                cost += transmission_cost
            elif data_source_name == "runtime":
                runtime += transmission_cost
            elif data_source_name == "carbon":
                carbon += transmission_cost

        deployments: list[tuple[dict[str, str], float, float, float]] = []

        for i, region in enumerate(regions):
            deployment_assignments: dict = {}
            for function in self._workflow_config.functions:
                deployment_assignments[function] = region

            if self._fail_hard_resource_constraints(self._workflow_config.constraints, cost[i], runtime[i], carbon[i]):
                continue

            deployments.append((deployment_assignments, cost[i], runtime[i], carbon[i]))

        return deployments
