import numpy as np

from multi_x_serverless.routing.solver.solver import Solver


class SimpleSolver(Solver):
    # Input:
    #   regions: list[dict] = [{"provider": "p1", "region": "r1"}, {"provider": "p2", "region": "r2"}]
    # Return values: list[tuple[dict[str, str], float, float, float]] = [
    #     ({index_instance: the_region_it_is_placed, ...# of instances more },  cost: float, runtime: float, carbon: float),
    #    ...# of regions more
    # ]
    def _solve(self, regions: list[dict]) -> list[tuple[dict, float, float, float]]:
        average_case_deployments: list[tuple[dict[int, int], float, float, float]] = []

        # list of indices of regions that are permitted for all functions
        permitted_regions: set[int] = set()

        for region in regions:
            region_index = self._region_indexer.get_value_indices()[(region["provider"], region["region"])]
            temp_permitted_regions = self._get_permitted_region_indices(regions, region_index)
            if not permitted_regions:
                permitted_regions = set(temp_permitted_regions)
            else:
                permitted_regions = permitted_regions.intersection(temp_permitted_regions)

        if not permitted_regions:
            raise Exception("There are no permitted regions")

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
