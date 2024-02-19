from multi_x_serverless.routing.distribution_solver.data_type.distribution import Distribution
from multi_x_serverless.routing.distribution_solver.data_type.sample_based_distribution import SampleBasedDistribution
from multi_x_serverless.routing.distribution_solver.distribution_solver import DistributionSolver


class DistributionBFSFineGrainedSolver(DistributionSolver):
    def _distribution_solve(
        self, regions: list[str]
    ) -> list[
        tuple[
            int, tuple[Distribution, Distribution, Distribution], tuple[float, float, float], tuple[float, float, float]
        ]
    ]:
        CurrentDistributionType = SampleBasedDistribution  # Set the distribution type

        # Get the home region index -> this is the region that the workflow starts from
        # For now the current implementation only supports one home region
        home_region_index = self._region_indexer.get_value_indices()[self._workflow_config.start_hops]

        # Denotes the nodes that have been processed - used for clearing memory
        processed_node_indices: set[int] = set()

        # Get the prerequisites and successor dictionaries
        prerequisites_dictionary, successor_dictionary = self._acquire_prerequisite_successor_dictionaries()

        # Get the permitted regions for the workflow
        permitted_regions = self._acquire_permitted_region_indices(regions)

        # Final deployments
        final_deployments: list[
            tuple[
                int,
                tuple[Distribution, Distribution, Distribution],
                tuple[float, float, float],
                tuple[float, float, float],
            ]
        ] = []

        for current_region_index in permitted_regions:
            ## Save in the format of dict(index_of_node, possible deployments list
            ## (dict(index_of_node, index_of_region), runtime_distribution, wc_runtime, pc_runtime))
            deployments: dict[int, tuple[Distribution, float, float]] = {}

            # The following variables are the current cost and carbon of the BFS

            ## Worse case and Experimental Probabilitic Case cost and carbon
            cumulative_wc_cost = cumulative_wc_carbon = cumulative_pc_cost = cumulative_pc_carbon = 0.0

            # Cummulative Distribution of the cost and carbon
            cumulative_cost_distribution: Distribution = CurrentDistributionType()
            cumulative_carbon_distribution: Distribution = CurrentDistributionType()

            for current_instance_index in self._topological_order:
                # Get the prerequisites of the current node
                prerequisites_indices: list[int] = prerequisites_dictionary[current_instance_index]

                # Hold the retrived previous deployments
                previous_deployments: list[tuple[tuple[Distribution, float, float], int]] = []

                # Aquire the previous deployments
                if len(prerequisites_indices) == 0:
                    # Special Case where the current node is a head node
                    blank_runtime_distribution: Distribution = CurrentDistributionType()
                    previous_deployments.append(((blank_runtime_distribution, 0.0, 0.0), -1))
                else:
                    # General Case where the current node is not a head node
                    for prerequisite_index in prerequisites_indices:
                        # Get the previous deployment
                        previous_deployments.append((deployments[prerequisite_index], prerequisite_index))

                held_runtime_distributions: list[Distribution] = []
                held_wc_runtime = held_pc_runtime = 0.0
                for deployment_package in previous_deployments:
                    # Get the previous instance index
                    deployment_values, previous_instance_index = deployment_package
                    previous_runtime_distribution, previous_wc_runtime, previous_pc_runtime = deployment_values
                    previous_region_index = current_region_index if previous_instance_index != -1 else home_region_index

                    # Get the transmission cost, carbon and runtime distribution
                    (
                        current_transmission_cost_distribution,
                        current_transmission_carbon_distribution,
                        current_transmission_runtime_distribution,
                    ) = self._input_manager.get_transmission_cost_carbon_runtime_distribution(
                        previous_instance_index, current_instance_index, previous_region_index, current_region_index
                    )

                    # Process the cost and carbon
                    cumulative_cost_distribution += current_transmission_cost_distribution
                    cumulative_carbon_distribution += current_transmission_carbon_distribution

                    cumulative_wc_carbon += current_transmission_carbon_distribution.get_tail_latency(True)
                    cumulative_wc_cost += current_transmission_cost_distribution.get_tail_latency(True)
                    cumulative_pc_carbon += current_transmission_carbon_distribution.get_average(False)
                    cumulative_pc_cost += current_transmission_cost_distribution.get_average(False)

                    # Process the runtime distribution and get the worst case runtime
                    held_runtime_distributions.append(
                        previous_runtime_distribution + current_transmission_runtime_distribution
                    )
                    held_wc_runtime = max(
                        held_wc_runtime,
                        previous_wc_runtime + current_transmission_runtime_distribution.get_tail_latency(True),
                    )
                    held_pc_runtime = max(
                        held_pc_runtime,
                        previous_pc_runtime + current_transmission_runtime_distribution.get_average(False),
                    )

                # Process the current runtime
                cumulative_runtime_distribution = CurrentDistributionType().get_merged_distribution(
                    held_runtime_distributions
                )

                # Process current execution if not virtual leaf node
                if current_instance_index != -1:
                    # Get the execution cost, carbon and runtime distribution
                    (
                        current_execution_cost_distribution,
                        current_execution_carbon_distribution,
                        current_execution_runtime_distribution,
                    ) = self._input_manager.get_execution_cost_carbon_runtime_distribution(
                        current_instance_index, current_region_index
                    )

                    # Process the cost and carbon
                    cumulative_cost_distribution += current_execution_cost_distribution
                    cumulative_carbon_distribution += current_execution_carbon_distribution

                    cumulative_wc_carbon += current_execution_carbon_distribution.get_tail_latency(True)
                    cumulative_wc_cost += current_execution_cost_distribution.get_tail_latency(True)
                    cumulative_pc_carbon += current_execution_carbon_distribution.get_average(False)
                    cumulative_pc_cost += current_execution_cost_distribution.get_average(False)

                    # Process the runtime distribution and get the worst case runtime
                    cumulative_runtime_distribution += current_execution_runtime_distribution
                    held_wc_runtime += cumulative_runtime_distribution.get_tail_latency(True)
                    held_pc_runtime += cumulative_runtime_distribution.get_average(False)

                    # Save the deployment
                    deployments[current_instance_index] = (
                        cumulative_runtime_distribution,
                        held_wc_runtime,
                        held_pc_runtime,
                    )
                else:
                    # Here we have the final distribution
                    final_deployments.append(
                        (
                            current_region_index,
                            (
                                cumulative_cost_distribution,
                                cumulative_runtime_distribution,
                                cumulative_carbon_distribution,
                            ),
                            (cumulative_wc_cost, held_wc_runtime, cumulative_wc_carbon),
                            (cumulative_pc_cost, held_pc_runtime, cumulative_pc_carbon),
                        )
                    )

                # Manage memory
                self._manage_memory(deployments, successor_dictionary, prerequisites_indices, processed_node_indices)

        return final_deployments

    def _manage_memory(
        self,
        deployments: dict[int, tuple[Distribution, float, float]],
        successor_dictionary: dict[int, list[int]],
        prerequisites_indices: list[int],
        processed_node_indices: set[int],
    ) -> None:
        # Clear memory of previous node
        for previous_instance_index in prerequisites_indices:
            previous_successor_indices: list[int] = successor_dictionary.get(previous_instance_index, [])
            if len(previous_successor_indices) == 1:  # If there is only one successor, we can clear the memory
                del deployments[previous_instance_index]
            elif (
                len(previous_successor_indices) > 1
            ):  # If there are multiple successors, we need to check if we can clear the memory
                # Check if all successors have been processed
                all_successors_processed = True
                for successor_index in previous_successor_indices:
                    if successor_index not in processed_node_indices:
                        all_successors_processed = False
                        break

                if all_successors_processed:
                    del deployments[previous_instance_index]

    def _acquire_prerequisite_successor_dictionaries(self) -> tuple[dict[int, list[int]], dict[int, list[int]]]:
        # Get the topological representation of a DAG
        prerequisites_dictionary = self._dag.get_prerequisites_dict()
        successor_dictionary = self._dag.get_preceeding_dict()

        # Add virtual leaf nodes to the DAG
        leaf_nodes = self._dag.get_leaf_nodes()
        if len(leaf_nodes) > 0:  # If there are leaf nodes (There should always be leaf nodes)
            self._topological_order.append(-1)
            for leaf_node in leaf_nodes:
                if -1 not in prerequisites_dictionary:
                    prerequisites_dictionary[-1] = []
                prerequisites_dictionary[-1].append(leaf_node)
                successor_dictionary[leaf_node].append(-1)
        else:
            raise ValueError("There are no leaf nodes in the DAG")

        return prerequisites_dictionary, successor_dictionary

    def _acquire_permitted_region_indices(self, regions: list[str]) -> set[int]:
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

        return permitted_regions
