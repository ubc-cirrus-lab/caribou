from multi_x_serverless.routing.solver.solver import Solver


class CoarseGrainedSolver(Solver):
    def _solve(self, regions: list[dict]) -> list[tuple[dict, float, float, float]]:
        average_case_deployments: list[tuple[dict[int, int], float, float, float]] = []

        # list of indices of regions that are permitted for all functions
        permitted_regions: set[int] = set()

        for current_instance_index in self._topological_order:
            temp_permitted_regions = self._get_permitted_region_indices(regions, current_instance_index)
            if not permitted_regions:
                permitted_regions = set(temp_permitted_regions)
            else:
                permitted_regions = permitted_regions.intersection(temp_permitted_regions)

        if not permitted_regions:
            raise ValueError("No permitted regions for any of the functions")

        for permitted_region in permitted_regions:
            current_deployment = self.init_deployment_to_region(permitted_region)

            if (
                not self._fail_hard_resource_constraints(
                    self._workflow_config.constraints,
                    current_deployment[1][1],
                    current_deployment[2][1],
                    current_deployment[3][1],
                )
                and (
                    current_deployment[0].copy(),
                    current_deployment[1][0],
                    current_deployment[2][0],
                    current_deployment[3][0],
                )
                not in average_case_deployments
            ):
                average_case_deployments.append(
                    (
                        current_deployment[0].copy(),
                        current_deployment[1][0],
                        current_deployment[2][0],
                        current_deployment[3][0],
                    )
                )

        return average_case_deployments
