import itertools

import numpy as np

from multi_x_serverless.routing.solver.solver import Solver


class TopologicalSolver(Solver):
    def _solve(self, regions: list[dict]) -> list[tuple[dict, float, float, float]]:
        # Get the topological representation of a DAG
        topological_order = self._dag.topological_sort()
        prerequisites_dictionary = self._dag.get_prerequisites_dict()
        successor_dictionary = self._dag.get_preceeding_dict()
        processed_node_indicies = set()  # Denotes the nodes that have been processed - used for clearing memory

        # Get the home region index -> this is the region that the workflow starts from
        # For now the current implementation only supports one home region
        home_region = self._workflow_config.home_regions[0]
        home_region_index = self._region_indexer.get_value_indices()[(home_region["provider"], home_region["region"])]

        # Add virtual leaf nodes to the DAG
        leaf_nodes = self._dag.get_leaf_nodes()
        if len(leaf_nodes) > 0:  # If there are leaf nodes (There should always be leaf nodes)
            topological_order.append(-1)
            for leaf_node in leaf_nodes:
                if -1 not in prerequisites_dictionary:
                    prerequisites_dictionary[-1] = []
                prerequisites_dictionary[-1].append(leaf_node)
                successor_dictionary[leaf_node].append(-1)
        else:
            raise Exception("There are no leaf nodes in the DAG")

        # Where its in format of {instance_index: 
        # [(deployment decisions (and cost and latency at that instance node), 
        # cumulative worse case cost/co2/runtime, probabilitic case runtime)]}
        deployments: dict[int, list[
            tuple[dict[str, (str, tuple[float, float], tuple[float, float])], 
            tuple[float, float, float], float]]] = {}
        for current_instance_index in topological_order:
            # print("Current Instance Index:", current_instance_index)

            # Instance flow related information
            prerequisites_indices: list[int] = prerequisites_dictionary[current_instance_index]
            
            # serverless region related information - per instance level
            # Where start hop and end hop should be already integrated into restrictions
            permitted_regions: list[dict[(str, str)]] = self._filter_regions_instance(regions, current_instance_index)
            if len(permitted_regions) == 0:  # Should never happen in a valid DAG
                raise Exception("There are no permitted regions for this instance")

            all_regions_indices = self._region_indexer.get_value_indices()
            permitted_regions_indices = np.array(
                [all_regions_indices[(region["provider"], region["region"])] for region in permitted_regions]
            )

            # List of sets of previous calculated instances
            current_deployments: list[tuple[dict[str, (str, tuple[float, float], tuple[float, float])], 
            tuple[float, float, float], float]] = []
            number_of_previous_instances = len(prerequisites_indices)
            if (number_of_previous_instances == 0):
                # print("No previous instances")
                # This is the start node
                from_region_index = home_region_index
                for to_region_index in permitted_regions_indices:
                    # Calculate the carbon/cost/runtime for transmission and execution
                    # For worse case (Using tail latency)
                    wc_t_cost, wc_t_carbon, wc_t_runtime = self._input_manager.get_transmission_cost_carbon_runtime(
                        None, current_instance_index, from_region_index, to_region_index)

                    wc_e_cost, wc_e_carbon, wc_e_runtime = self._input_manager.get_execution_cost_carbon_runtime(
                        current_instance_index, to_region_index)
                    
                    # Calculate the carbon/cost/runtime for transmission and execution
                    # For porbabilistic case (Using average latency and factor in invocation probability)
                    pc_t_cost, pc_t_carbon, pc_t_runtime = self._input_manager.get_transmission_cost_carbon_runtime(
                        None, current_instance_index, from_region_index, to_region_index, True)
                    
                    pc_e_cost, pc_e_carbon, pc_e_runtime = self._input_manager.get_execution_cost_carbon_runtime(
                        current_instance_index, to_region_index, True)

                    wc_cost = wc_t_cost + wc_e_cost
                    wc_carbon = wc_t_carbon + wc_e_carbon
                    wc_runtime = wc_t_runtime + wc_e_runtime

                    pc_cost = pc_t_cost + pc_e_cost
                    pc_carbon = pc_t_carbon + pc_e_carbon
                    pc_runtime = pc_t_runtime + pc_e_runtime

                    current_deployments.append(({
                        current_instance_index: (to_region_index, (wc_cost, wc_carbon), (pc_cost, pc_carbon)),
                    }, 
                    (wc_cost, wc_carbon, wc_runtime), pc_runtime))
            # elif (number_of_previous_instances == 1):
            #     print("One previous instance")
            #     # In this case there is only one previous instance
            #     # So we can just copy the previous deployment
            #     previous_instance_index = prerequisites_indices[0]
            #     previous_deployments = deployments[previous_instance_index]
            #     for previous_deployment in previous_deployments:
            #         previous_instance_placements, previous_wc_ccr, previous_pc_runtime = previous_deployment

            #         from_region_index = previous_instance_placements[previous_instance_index][0]
            #         for to_region_index in permitted_regions_indices:
            #             # Calculate the carbon/cost/runtime for transmission and execution
            #             # For worse case (Using tail latency)
            #             wc_t_cost, wc_t_carbon, wc_t_runtime = self._input_manager.get_transmission_cost_carbon_runtime(
            #                 previous_instance_index, current_instance_index, from_region_index, to_region_index)

            #             wc_e_cost, wc_e_carbon, wc_e_runtime = self._input_manager.get_execution_cost_carbon_runtime(
            #                 current_instance_index, to_region_index)
                        
            #             # Calculate the carbon/cost/runtime for transmission and execution
            #             # For porbabilistic case (Using average latency and factor in invocation probability)
            #             pc_t_cost, pc_t_carbon, pc_t_runtime = self._input_manager.get_transmission_cost_carbon_runtime(
            #                 previous_instance_index, current_instance_index, from_region_index, to_region_index, True)
                        
            #             pc_e_cost, pc_e_carbon, pc_e_runtime = self._input_manager.get_execution_cost_carbon_runtime(
            #                 current_instance_index, to_region_index, True)

            #             wc_cost_current = wc_t_cost + wc_e_cost
            #             wc_carbon_current = wc_t_carbon + wc_e_carbon
            #             wc_runtime_current = wc_t_runtime + wc_e_runtime

            #             pc_cost_current = pc_t_cost + pc_e_cost
            #             pc_carbon_current = pc_t_carbon + pc_e_carbon
            #             pc_runtime_current = pc_t_runtime + pc_e_runtime

            #             # Total Values
            #             wc_cost_total = previous_wc_ccr[0] + wc_cost_current
            #             wc_carbon_total = previous_wc_ccr[1] + wc_carbon_current
            #             wc_runtime_total = previous_wc_ccr[2] + wc_runtime_current

            #             pc_runtime_total = previous_pc_runtime + pc_runtime_current

            #             # Using copy method
            #             new_instance_placements = previous_instance_placements.copy()
            #             new_instance_placements[current_instance_index] = (to_region_index, (wc_cost_current, wc_carbon_current), (pc_cost_current, pc_carbon_current))
                        

            #             current_deployments.append((new_instance_placements, 
            #                 (wc_cost_total, wc_carbon_total, wc_runtime_total), pc_runtime_total))
                        
            #             # print("New addition:", (wc_cost_total, wc_carbon_total, wc_runtime_total), pc_runtime_total)
            # elif (number_of_previous_instances > 1):
            else:
                # print("Multiple previous instances")
                # Here we need an special handling of the case where there are multiple previous instances
                
                # First we need to find the common keys between the previous instances
                # This is just so we can compare the previous deployments
                predecessor_previous_instances: list[set[int]] = []
                for previous_instance_index in prerequisites_indices:
                    predecessor_previous_instances.append(set(deployments[previous_instance_index][0][0].keys()))
                
                # Find the common keys between the previous instances
                common_past_instance_keys = self._find_common_elements(predecessor_previous_instances)

                # Now we can group the previous deployments by the common keys
                deployment_groups: dict[frozenset[int, int], list[list]] = {}
                pred_index_counter = 0
                for previous_instance_index in prerequisites_indices:
                    for previous_deployment in deployments[previous_instance_index]:
                        common_keys = frozenset((k, previous_deployment[0][k][0]) for k in common_past_instance_keys if k in previous_deployment[0])

                        if common_keys not in deployment_groups:
                            deployment_groups[common_keys] = []

                        while len(deployment_groups[common_keys]) <= pred_index_counter:
                            deployment_groups[common_keys].append([])

                        deployment_groups[common_keys][pred_index_counter].append((previous_deployment, previous_instance_index))

                    pred_index_counter += 1
                if current_instance_index == -1:  # If this is the virtual end node
                    for common_keys, deployment_group in deployment_groups.items():
                        # Here is the format of the final deployment options
                        # In the future this will be using the average conditional dag results
                        # For now we just use the worse case (Until we implement conditional dag support)
                        final_deployments: list[tuple[dict, float, float, float]] = []
                        for combination in itertools.product(*deployment_group):
                            # For each combination of deployments, merge them together
                            list_of_dicts = [] # Debug code

                            combined_placements = {}
                            max_wc_runtime = 0
                            max_pc_runtime = 0
                            for (original_deployment_placement, wc_ccr, previous_pc_runtime), previous_instance_index in combination:
                                from_region_index = original_deployment_placement.get(
                                    previous_instance_index, None)[0]  # Prev should always be either in the dag or be home region
                                
                                previous_wc_runtime = wc_ccr[2]

                                list_of_dicts.append(original_deployment_placement) # For debug

                                # Merge the deployments information together
                                combined_placements = combined_placements | original_deployment_placement

                                max_wc_runtime = max(
                                    max_wc_runtime, previous_wc_runtime
                                )

                                max_pc_runtime = max(
                                    max_pc_runtime, previous_pc_runtime
                                )

                            # We need to now recalculate the cost and carbon for the combined placements
                            # As this is a potential merge node, here we also need a clean placemnet dict for results
                            wc_cost, wc_carbon, pc_cost, pc_carbon, clean_combined_placments = self._calculate_wc_pc_cost_carbon_cl_placements(combined_placements)
                            
                            
                            if not self._fail_hard_resource_constraints(
                                self._workflow_config.constraints, wc_cost, wc_carbon, max_wc_runtime
                            ):
                                # For now we use worse case, but when proability is implemented we will use that instead
                                final_deployments.append((clean_combined_placments, wc_cost, wc_carbon, max_wc_runtime))
                    # print("Final Deployments:", deployments)

                    # for index, deployment in deployments.items():
                    #     print("Deployment:", index, deployment)

                    del deployments # Clear all memory
                    
                    return final_deployments
                else:
                    for to_region_index in permitted_regions_indices:
                        for common_keys, deployment_group in deployment_groups.items():
                            # print("\n\ncommon_keys:", common_keys)
                            for combination in itertools.product(*deployment_group):
                                # print("combination:", combination)
                                # For each combination of deployments, merge them together
                                list_of_dicts = [] # Debug code
                                combined_placements = {}
                                max_wc_runtime = 0
                                max_pc_runtime = 0
                                
                                wc_cost_total = wc_carbon_total = wc_runtime_total = 0
                                pc_runtime_total = 0

                                # Calcualte the cost, carbon and runtime of execuion (Just execution here as its a shared value)
                                wc_e_cost, wc_e_carbon, wc_e_runtime = self._input_manager.get_execution_cost_carbon_runtime(
                                    current_instance_index, to_region_index)

                                pc_e_cost, pc_e_carbon, pc_e_runtime = self._input_manager.get_execution_cost_carbon_runtime(
                                    current_instance_index, to_region_index, True)
                                
                                # Transmission is is how much it cost to get from EVERY previous instance to this instance
                                # So we need to calculate the transmission cost for each previous instance
                                current_cumulative_wc_t_cost = current_cumulative_wc_t_carbon = 0
                                current_cumulative_pc_t_cost = current_cumulative_pc_t_carbon = 0

                                # This is the current max runtime from transition for all combinations of
                                # Previous instances to current region
                                current_max_wc_t_runtime = current_max_pc_t_runtime = 0
                                for (original_deployment_placement, wc_ccr, previous_pc_runtime), previous_instance_index in combination:
                                    from_region_index = original_deployment_placement.get(
                                        previous_instance_index, None)[0]  # Prev should always be either in the dag or be home region
                                    previous_wc_runtime = wc_ccr[2]

                                    # Calculate the carbon/cost/runtime for transmission
                                    # For worse case (Using tail latency)
                                    wc_t_cost, wc_t_carbon, wc_t_runtime = self._input_manager.get_transmission_cost_carbon_runtime(
                                        previous_instance_index, current_instance_index, from_region_index, to_region_index)
                                    
                                    current_cumulative_wc_t_cost += wc_t_cost
                                    current_cumulative_wc_t_carbon += wc_t_carbon

                                    # For porbabilistic case (Using average latency and factor in invocation probability)
                                    pc_t_cost, pc_t_carbon, pc_t_runtime = self._input_manager.get_transmission_cost_carbon_runtime(
                                        previous_instance_index, current_instance_index, from_region_index, to_region_index, True)
                                    
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
                                    list_of_dicts.append(original_deployment_placement) # For debug
                                    combined_placements = combined_placements | original_deployment_placement

                            
                                    max_wc_runtime = max(
                                        max_wc_runtime, wc_runtime_total
                                    )

                                    max_pc_runtime = max(
                                        max_pc_runtime, pc_runtime_total
                                    )

                                    current_max_wc_t_runtime = max(
                                        current_max_wc_t_runtime, wc_t_runtime
                                    )

                                    current_max_pc_t_runtime = max(
                                        current_max_pc_t_runtime, pc_t_runtime
                                    )
                                
                                # Get the current total cost and carbon for this specific transition (runtime doesnt matter for this)
                                current_instance_wc_cost = current_cumulative_wc_t_cost + wc_e_cost
                                current_instance_wc_carbon = current_cumulative_wc_t_carbon + wc_e_carbon

                                current_instance_pc_cost = current_cumulative_pc_t_cost + pc_e_cost
                                current_instance_pc_carbon = current_cumulative_pc_t_carbon + pc_e_carbon

                                # Append current key to combined placements
                                combined_placements[current_instance_index] = (to_region_index, 
                                                                                (current_instance_wc_cost, current_instance_wc_carbon), 
                                                                                (current_instance_pc_cost, current_instance_pc_carbon))
                                

                                # If this is a merge node, we need to calculate cost and carboned differently
                                wc_cost_total, wc_carbon_total, pc_cost_total, pc_carbon_total, _ = self._calculate_wc_pc_cost_carbon_cl_placements(combined_placements)

                                if not self._fail_hard_resource_constraints(
                                    self._workflow_config.constraints, wc_cost_total, wc_carbon_total, max_wc_runtime
                                ):
                                    current_deployments.append((combined_placements, 
                                        (wc_cost_total, wc_carbon_total, max_wc_runtime), max_pc_runtime))

                # return []
                        # have_different_values_for_common_keys(list_of_dicts)
                    # print("deployment_group:", deployment_group)
                    # prin
                # return []



            #     previous_deployments: list[
            #         list[tuple[dict[int, list[
            #         tuple[dict[str, (str, tuple[float, float], tuple[float, float])], 
            #         tuple[float, float, float], tuple[float, float, float]]]],
            #         int, set[int]]]
            #         ] = []

                

            



            # for previous_instance_index in prerequisites_indices:
            #     previous_deployment = []
            #     for deployment in deployments[previous_instance_index]:
            #         previous_deployment.append((deployment, previous_instance_index))
            #         prev_options_length += 1

            #     previous_deployments.append(previous_deployment)

            # if (
            #     len(previous_deployments) == 0
            # ):  # If there is no previous deployment, this is the start node and thus we initialise the first tuple.
            #     previous_deployments.append([(({}, (0, 0, 0), (0, 0, 0)), -1, set())])

            # combined_deployments: list[tuple[dict[str, str], tuple[float, float, float], tuple[float, float, float]]] = []
            # if current_instance_index == -1:  # If this is the virtual end node
            #     for combination in itertools.product(*previous_deployments):
            #         # For each combination of deployments, merge them together
            #         combined_placements: dict = {}
            #         sum_cost = sum_carbon = max_latency = 0
            #         for (
            #             original_deployment_placement,
            #             original_cost,
            #             original_carbon,
            #             original_runtime,
            #         ), previous_instance_index in combination:
            #             # Merge the deployments information together
            #             combined_placements = combined_placements | original_deployment_placement
            #             sum_cost += original_cost
            #             sum_carbon += original_carbon
            #             max_latency = max(max_latency, original_runtime)
            #         if not self._fail_hard_resource_constraints(
            #             self._workflow_config.constraints, sum_cost, max_latency, sum_carbon
            #         ):
            #             combined_deployments.append((combined_placements, sum_cost, sum_carbon, max_latency))
            # else:
                # for to_region_index in permitted_regions_indices:
                #     for combination in itertools.product(*previous_deployments):
                #         # For each combination of deployments, merge them together
                #         combined_placements = {}
                #         sum_cost = sum_carbon = max_latency = 0
                #         for (
                #             original_deployment_placement,
                #             original_cost,
                #             original_carbon,
                #             original_runtime,
                #         ), previous_instance_index in combination:
                #             from_region_index = original_deployment_placement.get(
                #                 previous_instance_index, None
                #             )  # Prev should always be either in the dag or be home region

                #             # Calculate transmission and execution cost/carbon/runtime from previous deployment region to this deployment region
                #             # First calculate transmission cost/carbon/runtime from previous deployment region to this deployment region
                #             (
                #                 transmission_cost,
                #                 transmission_carbon,
                #                 transmission_runtime,
                #             ) = self._input_manager.get_transmission_cost_carbon_runtime(
                #                 previous_instance_index, current_instance_index, from_region_index, to_region_index
                #             )

                #             # Then calculate execution cost/carbon/runtime for this deployment region
                #             (
                #                 execution_cost,
                #                 execution_carbon,
                #                 execution_runtime,
                #             ) = self._input_manager.get_execution_cost_carbon_runtime(
                #                 current_instance_index, to_region_index
                #             )

                #             # Merge the deployments information together
                #             combined_placements = combined_placements | original_deployment_placement
                #             sum_cost += original_cost + transmission_cost + execution_cost
                #             sum_carbon += original_cost + transmission_carbon + execution_carbon
                #             max_latency = max(
                #                 max_latency, (original_runtime + transmission_runtime + execution_runtime)
                #             )

                #             combined_placements[current_instance_index] = to_region_index

                #         if not self._fail_hard_resource_constraints(
                #             self._workflow_config.constraints, sum_cost, max_latency, sum_carbon
                #         ):
                #             combined_deployments.append((combined_placements, sum_cost, sum_carbon, max_latency))

            deployments[current_instance_index] = current_deployments
            processed_node_indicies.add(current_instance_index)

            # Clear memory of previous node
            for previous_instance_index in prerequisites_indices:
                previous_successor_indices: list[int] = successor_dictionary.get(previous_instance_index, [])
                if len(previous_successor_indices) == 1: # If there is only one successor, we can clear the memory
                    del deployments[previous_instance_index]
                # Need to consider to clear memory if there are multiple successors as well (only if it is no longer needed)
        
        # print(deployments[-1])
        return final_deployments
    
    def _find_common_elements(self, list_of_sets: list[set[int]]) -> list[int]:
        if not list_of_sets:
            return []

        common_elements = set(list_of_sets[0])

        for s in list_of_sets[1:]:
            common_elements.intersection_update(s)

        return list(common_elements)
    
    def _have_different_values_for_common_keys(list_of_dicts):
        common_key_values = {}

        # Iterate through the dictionaries and update common_key_values
        for current_dict in list_of_dicts:
            for key, value in current_dict.items():
                if key in common_key_values:
                    if common_key_values[key] != value:
                        return True  # Different values found for a common key
                else:
                    common_key_values[key] = value

        return False  # No different values found for any common key
    
    def _calculate_wc_pc_cost_carbon_cl_placements(self, instance_placement_data: dict[int, tuple[int, tuple[float, float], tuple[float, float]]]) -> tuple[float, float, float, float, dict[int, int]]:
        wc_cost = wc_carbon = 0
        pc_cost = pc_carbon = 0
        clean_placement_dict = {}

        for instance_index, (region_index, (wc_cost_instance, wc_carbon_instance), (pc_cost_instance, pc_carbon_instance)) in instance_placement_data.items():
            wc_cost += wc_cost_instance
            wc_carbon += wc_carbon_instance

            pc_cost += pc_cost_instance
            pc_carbon += pc_carbon_instance

            clean_placement_dict[instance_index] = region_index

        return wc_cost, wc_carbon, pc_cost, pc_carbon, clean_placement_dict
