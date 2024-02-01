import itertools
import numpy as np
import time
from typing import Optional

from multi_x_serverless.routing.solver.solver import Solver
from multi_x_serverless.routing.solver_inputs.input_manager import InputManager
from multi_x_serverless.routing.workflow_config import WorkflowConfig


PROFILE = False

if PROFILE:
    time_dic = {
        "_find_common_elements": 0,
        "_calculate_wc_pc_cost_carbon_cl_placements": 0,
        "prerequisites_indices_loop": 0,
        "non_end_n3_loop": 0,
    }


class BFSFineGrainedSolver(Solver):
    def __init__(
        self,
        workflow_config: WorkflowConfig,
        all_available_regions: Optional[list[dict]] = None,
        input_manager: Optional[InputManager] = None,
        init_home_region_transmission_costs: bool = True,
    ) -> None:
        super().__init__(workflow_config, all_available_regions, input_manager, False)

    def _solve(self, regions: list[dict]) -> list[tuple[dict, float, float, float]]:
        execution_cost_carbon_runtime_cache = {}
        transmission_cost_carbon_runtime_cache = {}

        # Get the topological representation of a DAG
        prerequisites_dictionary = self._dag.get_prerequisites_dict()
        successor_dictionary = self._dag.get_preceeding_dict()
        processed_node_indices = set()  # Denotes the nodes that have been processed - used for clearing memory

        # Get the home region index -> this is the region that the workflow starts from
        # For now the current implementation only supports one home region
        home_region = self._workflow_config.start_hops
        home_region_index = self._region_indexer.get_value_indices()[(home_region["provider"], home_region["region"])]

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
            raise Exception("There are no leaf nodes in the DAG")

        # Where its in format of {instance_index:
        # [(deployment decisions (and cost and latency at that instance node),
        # cumulative worse case cost/co2/runtime, probabilistic case runtime)]}
        deployments: dict[
            int,
            list[
                tuple[
                    dict[int, tuple[int, tuple[float, float], tuple[float, float]]], tuple[float, float, float], float
                ]
            ],
        ] = {}
        all_regions_indices = self._region_indexer.get_value_indices()
        for current_instance_index in self._topological_order:
            # print("\nCurrent Instance Index:", current_instance_index)

            # Instance flow related information
            prerequisites_indices: list[int] = prerequisites_dictionary[current_instance_index]

            # serverless region related information - per instance level
            # Where start hop and end hop should be already integrated into restrictions
            permitted_regions: list[dict[(str, str)]] = self._filter_regions_instance(regions, current_instance_index)
            if len(permitted_regions) == 0:  # Should never happen in a valid DAG
                raise Exception("There are no permitted regions for this instance")

            permitted_regions_indices = np.array(
                [all_regions_indices[(region["provider"], region["region"])] for region in permitted_regions]
            )

            # List of sets of previous calculated instances
            current_deployments: list[
                tuple[
                    dict[int, tuple[int, tuple[float, float], tuple[float, float]]], tuple[float, float, float], float
                ]
            ] = []
            number_of_previous_instances = len(prerequisites_indices)
            if number_of_previous_instances == 0:
                # This is the start node
                from_region_index = home_region_index
                for to_region_index in permitted_regions_indices:
                    # Calculate the carbon/cost/runtime for transmission and execution
                    # For worse case (Using tail latency)
                    lookup_key = (current_instance_index, to_region_index, False)
                    if lookup_key in execution_cost_carbon_runtime_cache.keys():
                        (wc_e_cost, wc_e_carbon, wc_e_runtime) = execution_cost_carbon_runtime_cache[lookup_key]
                    else:
                        wc_e_cost, wc_e_carbon, wc_e_runtime = self._input_manager.get_execution_cost_carbon_runtime(
                            current_instance_index, to_region_index, False
                        )
                        execution_cost_carbon_runtime_cache[lookup_key] = (wc_e_cost, wc_e_carbon, wc_e_runtime)

                    # Calculate the carbon/cost/runtime for transmission and execution # Do not consider start hop for now
                    # For probabilistic case (Using average latency and factor in invocation probability)
                    lookup_key = (current_instance_index, to_region_index, True)
                    if lookup_key in execution_cost_carbon_runtime_cache.keys():
                        pc_e_cost, pc_e_carbon, pc_e_runtime = execution_cost_carbon_runtime_cache[lookup_key]
                    else:
                        pc_e_cost, pc_e_carbon, pc_e_runtime = self._input_manager.get_execution_cost_carbon_runtime(
                            current_instance_index, to_region_index, True
                        )
                        execution_cost_carbon_runtime_cache[lookup_key] = (pc_e_cost, pc_e_carbon, pc_e_runtime)

                    # Start hop considerations
                    lookup_key = (
                        current_instance_index,
                        current_instance_index,
                        from_region_index,
                        to_region_index,
                        False,
                    )
                    if lookup_key in transmission_cost_carbon_runtime_cache.keys():
                        (wc_t_cost, wc_t_carbon, wc_t_runtime) = transmission_cost_carbon_runtime_cache.keys()[
                            lookup_key
                        ]
                    else:
                        (
                            wc_t_cost,
                            wc_t_carbon,
                            wc_t_runtime,
                        ) = self._input_manager.get_transmission_cost_carbon_runtime(
                            current_instance_index, current_instance_index, from_region_index, to_region_index, False
                        )
                        # cache the results
                        transmission_cost_carbon_runtime_cache[lookup_key] = (wc_t_cost, wc_t_carbon, wc_t_runtime)

                    wc_cost = wc_t_cost + wc_e_cost
                    wc_carbon = wc_t_carbon + wc_e_carbon
                    wc_runtime = wc_t_runtime + wc_e_runtime

                    # For probabilistic case (Using average latency and factor in invocation probability)
                    lookup_key = (
                        current_instance_index,
                        current_instance_index,
                        from_region_index,
                        to_region_index,
                        True,
                    )
                    if lookup_key in transmission_cost_carbon_runtime_cache.keys():
                        (pc_t_cost, pc_t_carbon, pc_t_runtime) = transmission_cost_carbon_runtime_cache.keys()[
                            lookup_key
                        ]
                    else:
                        (
                            pc_t_cost,
                            pc_t_carbon,
                            pc_t_runtime,
                        ) = self._input_manager.get_transmission_cost_carbon_runtime(
                            current_instance_index, current_instance_index, from_region_index, to_region_index, True
                        )
                        transmission_cost_carbon_runtime_cache[lookup_key] = (pc_t_cost, pc_t_carbon, pc_t_runtime)

                    pc_cost = pc_t_cost + pc_e_cost
                    pc_carbon = pc_t_carbon + pc_e_carbon
                    pc_runtime = pc_t_runtime + pc_e_runtime

                    current_deployments.append(
                        (
                            {current_instance_index: (to_region_index, (wc_cost, wc_carbon), (pc_cost, pc_carbon))},
                            (wc_cost, wc_carbon, wc_runtime),
                            pc_runtime,
                        )
                    )
            else:
                # Here we need an special handling of the case where there are multiple previous instances

                # First we need to find the common keys between the previous instances
                # This is just so we can compare the previous deployments
                predecessor_previous_instances: list[set[int]] = []
                for previous_instance_index in prerequisites_indices:
                    predecessor_previous_instances.append(set(deployments[previous_instance_index][0][0].keys()))

                # Find the common keys between the previous instances
                if PROFILE:
                    s = time.time()
                common_past_instance_keys = self._find_common_elements(predecessor_previous_instances)
                if PROFILE:
                    e = time.time()
                    time_dic["_find_common_elements"] += e - s

                # Now we can group the previous deployments by the common keys
                deployment_groups: dict[frozenset[tuple[int, int]], list[list]] = {}
                pred_index_counter = 0
                if PROFILE:
                    s = time.time()
                for previous_instance_index in prerequisites_indices:
                    for previous_deployment in deployments[previous_instance_index]:
                        common_keys = frozenset(
                            (k, previous_deployment[0][k][0])
                            for k in common_past_instance_keys
                            if k in previous_deployment[0]
                        )

                        if common_keys not in deployment_groups:
                            deployment_groups[common_keys] = [[] for _ in range(pred_index_counter + 1)]
                        else:
                            while len(deployment_groups[common_keys]) <= pred_index_counter:
                                deployment_groups[common_keys].append([])

                        deployment_groups[common_keys][pred_index_counter].append(
                            (previous_deployment, previous_instance_index)
                        )
                    pred_index_counter += 1
                if PROFILE:
                    e = time.time()
                    time_dic["prerequisites_indices_loop"] += e - s
                if current_instance_index == -1:  # If this is the virtual end node
                    final_deployments: list[tuple[dict, float, float, float]] = []
                    for common_keys, deployment_group in deployment_groups.items():
                        # Here is the format of the final deployment options
                        # In the future this will be using the average conditional dag results
                        # For now we just use the worse case (Until we implement conditional dag support)
                        for combination in itertools.product(*deployment_group):
                            # For each combination of deployments, merge them together
                            combined_placements: dict = {}
                            max_wc_runtime = 0.0
                            max_pc_runtime = 0.0
                            for (
                                (original_deployment_placement, wc_ccr, previous_pc_runtime),
                                previous_instance_index,
                            ) in combination:
                                from_region_index = original_deployment_placement.get(previous_instance_index, None)[
                                    0
                                ]  # Prev should always be either in the dag or be home region

                                previous_wc_runtime = wc_ccr[2]

                                # Merge the deployments information together
                                combined_placements = combined_placements | original_deployment_placement

                                max_wc_runtime = max(max_wc_runtime, previous_wc_runtime)

                                max_pc_runtime = max(max_pc_runtime, previous_pc_runtime)

                            # We need to now recalculate the cost and carbon for the combined placements
                            # As this is a potential merge node, here we also need a clean placement dict for results
                            if PROFILE:
                                s = time.time()
                            (
                                wc_cost,
                                wc_carbon,
                                pc_cost,
                                pc_carbon,
                                clean_combined_placements,
                            ) = self._calculate_wc_pc_cost_carbon_cl_placements(combined_placements)
                            if PROFILE:
                                e = time.time()
                                time_dic["_calculate_wc_pc_cost_carbon_cl_placements"] += e - s

                            if not self._fail_hard_resource_constraints(
                                self._workflow_config.constraints, wc_cost, max_wc_runtime, wc_carbon
                            ):
                                # For now we use worse case, but when probability is implemented we will use that instead
                                # Note to keep consistency with the other solvers, we save in cost, runtime, then carbon
                                final_deployments.append(
                                    (clean_combined_placements, pc_cost, max_pc_runtime, pc_carbon)
                                )

                    del deployments  # Clear all memory
                    if PROFILE:
                        print(time_dic)
                    return final_deployments
                else:  # Not the virtual end node
                    if PROFILE:
                        s227 = time.time()
                    for to_region_index in permitted_regions_indices:
                        for common_keys, deployment_group in deployment_groups.items():
                            for combination in itertools.product(*deployment_group):
                                # For each combination of deployments, merge them together
                                combined_placements = {}
                                max_wc_runtime = 0.0
                                max_pc_runtime = 0.0
                                wc_cost_total = 0.0
                                wc_carbon_total = 0.0
                                wc_runtime_total = 0.0
                                pc_runtime_total = 0.0

                                # Calculate the cost, carbon and runtime of execution (Just execution here as its a shared value)
                                lookup_key = (current_instance_index, to_region_index, False)
                                if lookup_key in execution_cost_carbon_runtime_cache.keys():
                                    wc_e_cost, wc_e_carbon, wc_e_runtime = execution_cost_carbon_runtime_cache[
                                        lookup_key
                                    ]
                                else:
                                    (
                                        wc_e_cost,
                                        wc_e_carbon,
                                        wc_e_runtime,
                                    ) = self._input_manager.get_execution_cost_carbon_runtime(
                                        current_instance_index, to_region_index, False
                                    )
                                    execution_cost_carbon_runtime_cache[lookup_key] = (
                                        wc_e_cost,
                                        wc_e_carbon,
                                        wc_e_runtime,
                                    )

                                lookup_key = (current_instance_index, to_region_index, True)
                                if lookup_key in execution_cost_carbon_runtime_cache.keys():
                                    pc_e_cost, pc_e_carbon, pc_e_runtime = execution_cost_carbon_runtime_cache[
                                        lookup_key
                                    ]
                                else:
                                    (
                                        pc_e_cost,
                                        pc_e_carbon,
                                        pc_e_runtime,
                                    ) = self._input_manager.get_execution_cost_carbon_runtime(
                                        current_instance_index, to_region_index, True
                                    )
                                    execution_cost_carbon_runtime_cache[lookup_key] = (
                                        pc_e_cost,
                                        pc_e_carbon,
                                        pc_e_runtime,
                                    )

                                # Transmission is is how much it cost to get from EVERY previous instance to this instance
                                # So we need to calculate the transmission cost for each previous instance
                                current_cumulative_wc_t_cost = 0.0
                                current_cumulative_wc_t_carbon = 0.0
                                current_cumulative_pc_t_cost = 0.0
                                current_cumulative_pc_t_carbon = 0.0

                                # This is the current max runtime from transition for all combinations of
                                # Previous instances to current region
                                current_max_wc_t_runtime = 0.0
                                current_max_pc_t_runtime = 0.0
                                for (
                                    (original_deployment_placement, wc_ccr, previous_pc_runtime),
                                    previous_instance_index,
                                ) in combination:
                                    from_region_index = original_deployment_placement.get(
                                        previous_instance_index, None
                                    )[
                                        0
                                    ]  # Prev should always be either in the dag or be home region
                                    previous_wc_runtime = wc_ccr[2]

                                    # Calculate the carbon/cost/runtime for transmission
                                    # For worse case (Using tail latency)
                                    lookup_key = (
                                        previous_instance_index,
                                        current_instance_index,
                                        from_region_index,
                                        to_region_index,
                                        False,
                                    )
                                    if lookup_key in transmission_cost_carbon_runtime_cache.keys():
                                        (wc_t_cost, wc_t_carbon, wc_t_runtime) = transmission_cost_carbon_runtime_cache[
                                            lookup_key
                                        ]
                                    else:
                                        (
                                            wc_t_cost,
                                            wc_t_carbon,
                                            wc_t_runtime,
                                        ) = self._input_manager.get_transmission_cost_carbon_runtime(
                                            previous_instance_index,
                                            current_instance_index,
                                            from_region_index,
                                            to_region_index,
                                            False,
                                        )
                                        transmission_cost_carbon_runtime_cache[lookup_key] = (
                                            wc_t_cost,
                                            wc_t_carbon,
                                            wc_t_runtime,
                                        )

                                    current_cumulative_wc_t_cost += wc_t_cost
                                    current_cumulative_wc_t_carbon += wc_t_carbon

                                    # For probabilistic case (Using average latency and factor in invocation probability)
                                    lookup_key = (
                                        previous_instance_index,
                                        current_instance_index,
                                        from_region_index,
                                        to_region_index,
                                        True,
                                    )
                                    if lookup_key in transmission_cost_carbon_runtime_cache.keys():
                                        (pc_t_cost, pc_t_carbon, pc_t_runtime) = transmission_cost_carbon_runtime_cache[
                                            lookup_key
                                        ]
                                    else:
                                        (
                                            pc_t_cost,
                                            pc_t_carbon,
                                            pc_t_runtime,
                                        ) = self._input_manager.get_transmission_cost_carbon_runtime(
                                            previous_instance_index,
                                            current_instance_index,
                                            from_region_index,
                                            to_region_index,
                                            True,
                                        )
                                        transmission_cost_carbon_runtime_cache[lookup_key] = (
                                            pc_t_cost,
                                            pc_t_carbon,
                                            pc_t_runtime,
                                        )

                                    current_cumulative_pc_t_cost += pc_t_cost
                                    current_cumulative_pc_t_carbon += pc_t_carbon

                                    # Get current total cost, carbon and runtime
                                    wc_cost_current = wc_t_cost + wc_e_cost
                                    wc_carbon_current = wc_t_carbon + wc_e_carbon
                                    wc_runtime_current = wc_t_runtime + wc_e_runtime

                                    # pc_cost_current = pc_t_cost + pc_e_cost
                                    # pc_carbon_current = pc_t_carbon + pc_e_carbon
                                    pc_runtime_current = pc_t_runtime + pc_e_runtime

                                    # Total Values
                                    wc_cost_total = wc_ccr[0] + wc_cost_current
                                    wc_carbon_total = wc_ccr[1] + wc_carbon_current
                                    wc_runtime_total = wc_ccr[2] + wc_runtime_current

                                    pc_runtime_total = previous_pc_runtime + pc_runtime_current

                                    # Merge the deployments information together
                                    combined_placements = combined_placements | original_deployment_placement

                                    max_wc_runtime = max(max_wc_runtime, wc_runtime_total)
                                    max_pc_runtime = max(max_pc_runtime, pc_runtime_total)

                                    current_max_wc_t_runtime = max(current_max_wc_t_runtime, wc_t_runtime)
                                    current_max_pc_t_runtime = max(current_max_pc_t_runtime, pc_t_runtime)

                                # Get the current total cost and carbon for this specific transition (runtime doesn't matter for this)
                                current_instance_wc_cost = current_cumulative_wc_t_cost + wc_e_cost
                                current_instance_wc_carbon = current_cumulative_wc_t_carbon + wc_e_carbon

                                current_instance_pc_cost = current_cumulative_pc_t_cost + pc_e_cost
                                current_instance_pc_carbon = current_cumulative_pc_t_carbon + pc_e_carbon

                                # Append current key to combined placements
                                combined_placements[current_instance_index] = (
                                    to_region_index,
                                    (current_instance_wc_cost, current_instance_wc_carbon),
                                    (current_instance_pc_cost, current_instance_pc_carbon),
                                )

                                if PROFILE:
                                    s = time.time()
                                (
                                    wc_cost_total,
                                    wc_carbon_total,
                                    pc_cost_total,
                                    pc_carbon_total,
                                    _,
                                ) = self._calculate_wc_pc_cost_carbon_cl_placements(combined_placements)
                                if PROFILE:
                                    e = time.time()
                                    time_dic["_calculate_wc_pc_cost_carbon_cl_placements"] += e - s

                                if not self._fail_hard_resource_constraints(
                                    self._workflow_config.constraints, wc_cost_total, max_wc_runtime, wc_carbon_total
                                ):
                                    current_deployments.append(
                                        (
                                            combined_placements,
                                            (wc_cost_total, wc_carbon_total, max_wc_runtime),
                                            max_pc_runtime,
                                        )
                                    )
                    if PROFILE:
                        e227 = time.time()
                        time_dic["non_end_n3_loop"] += e227 - s227

            deployments[current_instance_index] = current_deployments
            processed_node_indices.add(current_instance_index)

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

        # print(time_dic)
        return final_deployments

    def _find_common_elements(self, list_of_sets: list[set[int]]) -> set[int]:
        if not list_of_sets:
            return {}
        return set.intersection(*list_of_sets)

    def _calculate_wc_pc_cost_carbon_cl_placements(
        self, instance_placement_data: dict[int, tuple[int, tuple[float, float], tuple[float, float]]]
    ) -> tuple[float, float, float, float, dict[int, int]]:
        wc_cost = 0.0
        wc_carbon = 0.0
        pc_cost = 0.0
        pc_carbon = 0.0

        clean_placement_dict = {k: 0 for k in self._topological_order}

        for (
            instance_index,
            (region_index, (wc_cost_instance, wc_carbon_instance), (pc_cost_instance, pc_carbon_instance)),
        ) in instance_placement_data.items():
            wc_cost += wc_cost_instance
            wc_carbon += wc_carbon_instance
            pc_cost += pc_cost_instance
            pc_carbon += pc_carbon_instance
            clean_placement_dict[instance_index] = region_index

        return wc_cost, wc_carbon, pc_cost, pc_carbon, clean_placement_dict
