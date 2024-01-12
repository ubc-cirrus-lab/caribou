import itertools

import numpy as np

from multi_x_serverless.routing.solver.solver import Solver


class TopologicalSolver(Solver):
    def _solve(self, regions: list[dict]) -> list[tuple[dict, float, float, float]]:
        # Get the topological representation of a DAG
        topological_order = self._dag.topological_sort()
        prerequisites_dictionary = self._dag.get_prerequisites_dict()
        processed_node_indicies = set()  # Denotes the nodes that have been processed - used for clearing memory

        # Add virtual leaf nodes to the DAG
        leaf_nodes = self._dag.get_leaf_nodes()
        if len(leaf_nodes) > 0:  # If there are leaf nodes (There should always be leaf nodes)
            topological_order.append(-1)
            for leaf_node in leaf_nodes:
                if -1 not in prerequisites_dictionary:
                    prerequisites_dictionary[-1] = []
                prerequisites_dictionary[-1].append(leaf_node)
        else:
            raise Exception("There are no leaf nodes in the DAG")

        # Where its in (instance placements, cost, carbon, runtime)
        deployments: dict[int, list[tuple[dict[str, str], float, float, float]]] = {}
        for current_instance_index in topological_order:
            # Instance flow related information
            prerequisites_indices: list[int] = prerequisites_dictionary[current_instance_index]

            # serverless region related information - per instance level
            # Where start hop and end hop should be already integrated into restrictions
            permited_regions: list[dict[(str, str)]] = self._filter_regions_instance(regions, current_instance_index)
            if len(permited_regions) == 0:  # Should never happen in a valid DAG
                raise Exception("There are no permited regions for this instance")

            permited_regions_indices = self._region_indexer.values_to_indices(
                np.array([(region["provider"], region["region"]) for region in permited_regions])
            )

            # For testing purposes -> will be removed or transfered to logger.
            # print()
            # print("current_instance_index:", current_instance_index)
            # print("prerequisites_indices:", prerequisites_indices)
            # print("permited_regions_indices:", permited_regions_indices)

            # This is the current deployment in this iteration
            # Such that if we move all of previous deployment to any of the current
            # permitted regions, what are the deployments.
            # Get the previous deployments
            prev_options_length = 0
            previous_deployments: list = []
            for previous_instance_index in prerequisites_indices:
                # # For testing purposes -> will be removed or transfered to logger.
                # print(
                #     "previous deployment length of instance:",
                #     previous_instance_index,
                #     "-",
                #     len(deployments[previous_instance_index]),
                # )
                previous_deployment = []
                for deployment in deployments[previous_instance_index]:
                    previous_deployment.append((deployment, previous_instance_index))
                    prev_options_length += 1

                previous_deployments.append(previous_deployment)

            if (
                len(previous_deployments) == 0
            ):  # If there is no previous deployment, this is the start node and thus we initialise the first tuple. May be moved later.
                previous_deployments.append([(({}, 0, 0, 0), None)])

            combined_deployments: list[tuple[dict[str, str], float, float, float]] = []
            if current_instance_index == -1:  # If this is the virtual end node
                for combination in itertools.product(*previous_deployments):
                    # For each combination of deployments, merge them together
                    combined_placements: dict = {}
                    sum_cost = sum_carbon = max_latency = 0
                    for (
                        original_deployment_placement,
                        original_cost,
                        original_carbon,
                        original_runtime,
                    ), previous_instance_index in combination:
                        # Merge the deployments information together
                        combined_placements = combined_placements | original_deployment_placement
                        sum_cost += original_cost
                        sum_carbon += original_carbon
                        max_latency = max(max_latency, original_runtime)

                    combined_deployments.append((combined_placements, sum_cost, sum_carbon, max_latency))
            else:
                for to_region_index in permited_regions_indices:
                    for combination in itertools.product(*previous_deployments):
                        # For each combination of deployments, merge them together
                        combined_placements = {}
                        sum_cost = sum_carbon = max_latency = 0
                        for (
                            original_deployment_placement,
                            original_cost,
                            original_carbon,
                            original_runtime,
                        ), previous_instance_index in combination:
                            from_region_index = original_deployment_placement.get(previous_instance_index, None)

                            # Calculate transmission and execution cost/carbon/runtime from previous deployment region to this deployment region
                            # First calculate transmission cost/carbon/runtime from previous deployment region to this deployment region
                            (
                                transmission_cost,
                                transmission_carbon,
                                transmission_runtime,
                            ) = self._input_manager.get_transmission_cost_carbon_runtime(
                                previous_instance_index, current_instance_index, from_region_index, to_region_index
                            )

                            # Then calculate execution cost/carbon/runtime for this deployment region
                            (
                                execution_cost,
                                execution_carbon,
                                execution_runtime,
                            ) = self._input_manager.get_execution_cost_carbon_runtime(
                                current_instance_index, to_region_index
                            )

                            # Merge the deployments information together
                            combined_placements = combined_placements | original_deployment_placement
                            sum_cost += original_cost + transmission_cost + execution_cost
                            sum_carbon += original_cost + transmission_carbon + execution_carbon
                            max_latency = max(
                                max_latency, (original_runtime + transmission_runtime + execution_runtime)
                            )

                            combined_placements[current_instance_index] = to_region_index

                        combined_deployments.append((combined_placements, sum_cost, sum_carbon, max_latency))

            # # For testing purposes -> will be removed or transfered to logger.
            # print("New combined deployment length:", len(combined_deployments))
            deployments[current_instance_index] = combined_deployments
            processed_node_indicies.add(current_instance_index)

        # # For testing purposes -> will be removed or transfered to logger.
        # print("\n")
        return deployments[-1]  # Return the result for the virtual end node
