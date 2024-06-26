import random
from typing import Any, Optional

import numpy as np

from caribou.deployment_solver.deployment_input.components.calculator import InputCalculator
from caribou.deployment_solver.deployment_input.components.loaders.performance_loader import PerformanceLoader
from caribou.deployment_solver.deployment_input.components.loaders.workflow_loader import WorkflowLoader
from caribou.common.constants import (
    SOLVER_HOME_REGION_TRANSMISSION_LATENCY_DEFAULT
)

class RuntimeCalculator(InputCalculator):
    def __init__(self, performance_loader: PerformanceLoader, workflow_loader: WorkflowLoader) -> None:
        super().__init__()
        self._performance_loader: PerformanceLoader = performance_loader
        self._workflow_loader: WorkflowLoader = workflow_loader
        # self._workflow_loader._performance_loader = performance_loader
        self._transmission_latency_distribution_cache: dict[str, list[float]] = {}
        self._transmission_size_distribution_cache: dict[str, list[float]] = {}

    def reset_cache(self) -> None:
        self._transmission_latency_distribution_cache = {}
        self._transmission_size_distribution_cache = {}

    # def get_transmission_size_distribution(
    #     self,
    #     from_instance_name: Optional[str],
    #     to_instance_name: str,
    #     from_region_name: str,
    #     to_region_name: str,
    # ) -> list[float]:
    #     cache_key = f"{from_instance_name}-{to_instance_name}-{from_region_name}-{to_region_name}"
    #     if cache_key in self._transmission_size_distribution_cache:
    #         return self._transmission_size_distribution_cache[cache_key]
    #     # Get the data transfer size distribution
    #     if from_instance_name:
    #         transmission_size_distribution = self._workflow_loader.get_data_transfer_size_distribution(
    #             from_instance_name, to_instance_name, from_region_name, to_region_name
    #         )
    #         if len(transmission_size_distribution) == 0:
    #             # If the size distribution is empty, we default to the home region size distribution
    #             transmission_size_distribution = self._workflow_loader.get_data_transfer_size_distribution(
    #                 from_instance_name,
    #                 to_instance_name,
    #                 self._workflow_loader.get_home_region(),
    #                 self._workflow_loader.get_home_region(),
    #             )
    #     else:
    #         transmission_size_distribution = self._workflow_loader.get_start_hop_size_distribution(to_region_name)

    #     self._transmission_size_distribution_cache[cache_key] = transmission_size_distribution
    #     return transmission_size_distribution

    # def get_transmission_latency_distribution(
    #     self,
    #     from_instance_name: Optional[str],
    #     to_instance_name: str,
    #     from_region_name: str,
    #     to_region_name: str,
    #     data_transfer_size: Optional[float],
    # ) -> list[float]:
    #     cache_key = f"{from_instance_name}-{to_instance_name}-{from_region_name}-{to_region_name}-{data_transfer_size}"
    #     if cache_key in self._transmission_latency_distribution_cache:
    #         return self._transmission_latency_distribution_cache[cache_key]
    #     if data_transfer_size is not None:
    #         if from_instance_name:
    #             # Not for start hop
    #             # Get the data transfer size distribution
    #             transmission_latency_distribution = self._workflow_loader.get_latency_distribution(
    #                 from_instance_name, to_instance_name, from_region_name, to_region_name, data_transfer_size
    #             )

    #             if len(transmission_latency_distribution) == 0:
    #                 transmission_latency_distribution = self.get_performance_loader_transmission_latency_distribution(
    #                     from_region_name, to_region_name, from_instance_name, to_instance_name, data_transfer_size
    #                 )
    #         else:
    #             transmission_latency_distribution = self._workflow_loader.get_start_hop_latency_distribution(
    #                 to_region_name, data_transfer_size
    #             )
    #     else:
    #         assert (
    #             from_instance_name is not None
    #         ), "From instance name must be provided for underestimation factor, something went wrong."
    #         transmission_latency_distribution = self.get_performance_loader_transmission_latency_distribution(
    #             from_region_name, to_region_name, from_instance_name, to_instance_name
    #         )

    #     self._transmission_latency_distribution_cache[cache_key] = transmission_latency_distribution
    #     return transmission_latency_distribution

    # def calculate_runtime_distribution(self, instance_name: str, region_name: str) -> list[float]:
    #     runtime_distribution: list[float] = self._workflow_loader.get_runtime_distribution(instance_name, region_name)

    #     # If the runtime is not found, then we simply default to home region runtime
    #     if len(runtime_distribution) == 0:
    #         home_region = self._workflow_loader.get_home_region()

    #         # Currently we do not consider relative performance such that we
    #         # Simply assume it runs the same in all regions to its home region
    #         # At least for bootstrapping purposes
    #         runtime_distribution = self._workflow_loader.get_runtime_distribution(instance_name, home_region)

    #         # It is possible for a workflow to have no runtime distribution for a specific instance
    #         # This happens if the instance was never invoked in the workflow so far
    #         # such that they have no runtime data, in this case we assume the runtime is 0
    #         if len(runtime_distribution) == 0:
    #             runtime_distribution.append(0.0)

    #     return runtime_distribution

    # # Old performance loader approach
    # def get_performance_loader_transmission_latency_distribution(
    #     self,
    #     from_region_name: str,
    #     to_region_name: str,
    #     from_instance_name: str,
    #     to_instance_name: str,
    #     data_transfer_size: Optional[float] = None,
    # ) -> list[float]:
    #     # No size information, we default to performance loader
    #     transmission_latency_distribution = self._performance_loader.get_transmission_latency_distribution(
    #         from_region_name, to_region_name
    #     )

    #     # Since this will underestimate the latency, we multiply by the
    #     # underestimation factor retrieved from home region
    #     home_region_name = self._workflow_loader.get_home_region()
    #     home_region_latency_distribution_performance = self._performance_loader.get_transmission_latency_distribution(
    #         home_region_name, home_region_name
    #     )

    #     # Calculate average latency
    #     home_region_latency_performance = np.mean(home_region_latency_distribution_performance)

    #     home_region_transmission_size_distribution = self._workflow_loader.get_data_transfer_size_distribution(
    #         from_instance_name, to_instance_name, home_region_name, home_region_name
    #     )

    #     if data_transfer_size is None or data_transfer_size not in home_region_transmission_size_distribution:
    #         # Select a random size from the distribution
    #         home_region_transmission_size = home_region_transmission_size_distribution[
    #             int(random.random() * (len(home_region_transmission_size_distribution) - 1))
    #         ]
    #     else:
    #         home_region_transmission_size = data_transfer_size

    #     home_region_latency_distribution_measured = self._workflow_loader.get_latency_distribution(
    #         from_instance_name, to_instance_name, home_region_name, home_region_name, home_region_transmission_size
    #     )

    #     # Calculate the average latency
    #     home_region_latency_measured = np.mean(home_region_latency_distribution_measured)

    #     # Calculate the underestimation factor
    #     underestimation_factor = home_region_latency_measured / home_region_latency_performance

    #     assert isinstance(underestimation_factor, float), "Underestimation factor must be a float."

    #     # Apply the underestimation factor
    #     transmission_latency_distribution = [
    #         latency * underestimation_factor for latency in transmission_latency_distribution
    #     ]

    #     return transmission_latency_distribution

######### New functions #########
    def calculate_simulated_transmission_size_and_latency(self,
                from_instance_name: str,
                uninvoked_instance_name: str,
                simulated_sync_predecessor_name: str,
                sync_node_name: str,
                from_region_name: str,
                to_region_name: str,
    ) -> tuple[float, float]:
        sync_to_from_instance = f"{simulated_sync_predecessor_name}>{sync_node_name}"

        # Get the average transmission size of the from_instance to the sync node from
        # the non-execution information
        transmission_size = self._workflow_loader.get_non_execution_sns_transfer_size(from_instance_name, uninvoked_instance_name, sync_to_from_instance)

        # Get the non-execution transmission latency distribution of the input size (If it exists)
        transmission_latency_distribution: list[float] = self._workflow_loader.get_non_execution_transfer_latency_distribution(
            from_instance_name,
            uninvoked_instance_name,
            sync_to_from_instance,
            from_region_name,
            to_region_name
        )

        if len(transmission_latency_distribution) == 0:
            # TODO: MAKE SURE THIS WORKS IF NOT THEN USE AN ALTERNATIVE METHOD!!!
            # Default to transmission latency distribution of what happens when simulated_sync_predecessor_name
            # Calls sync_node_name as a normal transmission
            transmission_latency_distribution: list[float] = self._get_transmission_latency_distribution(
                simulated_sync_predecessor_name, from_region_name, sync_node_name, to_region_name, transmission_size
            )
            
        # print(f'SIMULATED transmission_latency_distribution: {transmission_latency_distribution[:5]}\n')

        # Pick a transmission latency
        transmission_latency: float = transmission_latency_distribution[
            int(random.random() * (len(transmission_latency_distribution) - 1))
        ]

        return transmission_size, transmission_latency


    def calculate_transmission_size_and_latency(self,
                                from_instance_name: Optional[str],
                                from_region_name: Optional[str],
                                to_instance_name: str,
                                to_region_name: str,
    ) -> tuple[float, float]:
        # Here we pick a random data transfer size, then pick a random latency

        # Get the transmission size distribution
        transmission_size_distribution: list[float] = self._get_transmission_size_distribution(
            from_instance_name, to_instance_name
        )
        
        # Pick a transmission size
        transmission_size: float = transmission_size_distribution[
            int(random.random() * (len(transmission_size_distribution) - 1))
        ]
        # # TODO: Remove print statements
        # if not from_instance_name:
        #     print("Instance Name: Start Hop")

        # print(f"transmission_size_distribution: {transmission_size_distribution}")

        # print(from_instance_name, to_instance_name, from_region_name, to_region_name, transmission_size)
        # Get the transmission latency distribution of the input size
        transmission_latency_distribution: list[float] = self._get_transmission_latency_distribution(
            from_instance_name, from_region_name, to_instance_name, to_region_name, transmission_size
        )
        # print(f'transmission_latency_distribution: {transmission_latency_distribution[:5]}\n')

        # Pick a transmission latency
        transmission_latency: float = transmission_latency_distribution[
            int(random.random() * (len(transmission_latency_distribution) - 1))
        ]

        # # TODO: Remove this
        # transmission_latency = 1.0

        return transmission_size, transmission_latency
    
    def _get_transmission_latency_distribution(
        self,
        from_instance_name: Optional[str],
        from_region_name: Optional[str],
        to_instance_name: str,
        to_region_name: str,
        data_transfer_size: float,
    ) -> list[float]:
        cache_key = f"{from_instance_name}-{to_instance_name}-{from_region_name}-{to_region_name}-{data_transfer_size}"
        if cache_key in self._transmission_latency_distribution_cache:
            return self._transmission_latency_distribution_cache[cache_key]
        
        if from_instance_name:
            transmission_latency_distribution = self._workflow_loader.get_latency_distribution(
                from_instance_name, to_instance_name, from_region_name, to_region_name, data_transfer_size
            )
            if len(transmission_latency_distribution) == 0:
                transmission_latency_distribution = self._handle_missing_transmission_latency_distribution(
                    from_instance_name, from_region_name, to_instance_name, to_region_name, data_transfer_size)
        else:
            transmission_latency_distribution = self._workflow_loader.get_start_hop_latency_distribution(
                to_region_name, data_transfer_size
            )
            if len(transmission_latency_distribution) == 0:
                transmission_latency_distribution = self._handle_missing_start_hop_latency_distribution(to_region_name, data_transfer_size)


        if len(transmission_latency_distribution) == 0:
            # There should never be a case where the size distribution is empty
            # As the missing handling should have taken care of it
            raise ValueError(f"The transmission latency distribution for {from_instance_name} to {to_instance_name} for {from_region_name} to {to_region_name} is empty, this should be impossible.")

        self._transmission_latency_distribution_cache[cache_key] = transmission_latency_distribution
        return transmission_latency_distribution

    def _handle_missing_transmission_latency_distribution(
            self, 
            from_instance_name: str, 
            from_region_name: str, 
            to_instance_name: str, 
            to_region_name: str, 
            data_transfer_size: float
        ) -> list[float]:
        # Temporarily use the old function for now
        # TODO: Implement the new function
        return self._get_performance_loader_transmission_latency_distribution(
            from_instance_name, from_region_name, to_instance_name, to_region_name, data_transfer_size
        )

    def _get_performance_loader_transmission_latency_distribution(
        self,
        from_instance_name: str,
        from_region_name: str,
        to_instance_name: str,
        to_region_name: str,
        data_transfer_size: float,
    ) -> list[float]:
        # Adapted from the old function
        # TODO: Replace this function if it is found to be incorrect

        # No size information, we default to performance loader
        transmission_latency_distribution = self._performance_loader.get_transmission_latency_distribution(
            from_region_name, to_region_name
        )

        # Since this will underestimate the latency, we multiply by the
        # underestimation factor retrieved from home region
        home_region_name = self._workflow_loader.get_home_region()
        home_region_latency_distribution_performance = self._performance_loader.get_transmission_latency_distribution(home_region_name, home_region_name)

        # Calculate average latency
        home_region_latency_performance: float = float(np.mean(home_region_latency_distribution_performance))

        # Get the measure latency from the home region
        home_region_latency_distribution_measured = self._workflow_loader.get_latency_distribution(
            from_instance_name, to_instance_name, home_region_name, home_region_name, data_transfer_size
        )
        if len(home_region_latency_distribution_measured) == 0:
            # For cases where its a sync predecessor, there might be no latency data
            # even for the home region, in this case we default to the average latency between
            # two of the same region (A default value)
            # TODO: Verify or find a better solution
            home_region_latency_distribution_measured = [SOLVER_HOME_REGION_TRANSMISSION_LATENCY_DEFAULT]

        # Calculate the average measured latency of the home region
        home_region_latency_measured: float = float(np.mean(home_region_latency_distribution_measured))

        # Calculate the underestimation factor
        underestimation_factor = home_region_latency_measured / home_region_latency_performance

        # # TODO: Remove this
        # underestimation_factor = 1.0

        assert isinstance(underestimation_factor, float), "Underestimation factor must be a float."

        # Apply the underestimation factor
        transmission_latency_distribution = [
            latency * underestimation_factor for latency in transmission_latency_distribution
        ]

        return transmission_latency_distribution

    def _handle_missing_start_hop_latency_distribution(self, to_region_name: str,
        data_transfer_size: float) -> list[float]:
        home_region_name = self._workflow_loader.get_home_region()
        if to_region_name == home_region_name:
            # If we don't even have start hop transmission latency for home region then
            # something is really wrong, raise error as this should be impossible
            # unless the workflow was NEVER invoked
            raise ValueError("Start hop latency distribution for home region is empty, this should be impossible.")

        # At this point we are in a region that the instance was never invoked
        # Thus we have no data transfer size distribution
        # In this case we try to estimate the latency using the transmission latency
        # Based on the transmission latency distribution, here we assume that we can estimate the latency
        # By adding estimated transmission latency (cloud ping) to the start hop latency
        home_region_latency_distribution = self._workflow_loader.get_start_hop_latency_distribution(home_region_name, data_transfer_size)
        transmission_latency_distribution = self._performance_loader.get_transmission_latency_distribution(
            home_region_name, to_region_name
        )
        start_hop_latency_distribution = [
            latency + transmission_latency_distribution[i % len(transmission_latency_distribution)]
            for i, latency in enumerate(home_region_latency_distribution)
        ]

        return start_hop_latency_distribution

    def _get_transmission_size_distribution(
        self,
        from_instance_name: Optional[str],
        to_instance_name: str,
    ) -> list[float]:
        cache_key = f"{from_instance_name}-{to_instance_name}"
        if cache_key in self._transmission_size_distribution_cache:
            return self._transmission_size_distribution_cache[cache_key]
        # Get the data transfer size distribution
        if from_instance_name:
            transmission_size_distribution = self._workflow_loader.get_data_transfer_size_distribution(from_instance_name, to_instance_name)
        else:
            transmission_size_distribution = self._workflow_loader.get_start_hop_size_distribution()

        if len(transmission_size_distribution) == 0:
            # There should never be a case where the size distribution is empty
            # As it would just mean that the instance was never invoked
            # But then it should never had reached this point (as it would have 0% inv probability)
            raise ValueError(f"Size distribution for {from_instance_name} to {to_instance_name} is empty, this should be impossible.")

        self._transmission_size_distribution_cache[cache_key] = transmission_size_distribution
        return transmission_size_distribution

    def calculate_node_runtimes_and_data_transfer(self, 
                              instance_name: str, 
                              region_name: str,
                              previous_cumulative_runtime: float
                            ) -> tuple[dict[str, Any], float]:
        
        # Calculate the current runtime of this instance when executed in the given region
        # Get the runtime distribution of the instance in the given region
        runtime_distribution: list[float] = self._workflow_loader.get_runtime_distribution(instance_name, region_name)
        original_runtime_region_name = desired_runtime_region_name = region_name
        if len(runtime_distribution) == 0:
            # No runtime data for this instance in this region, default to home region
            home_region = self._workflow_loader.get_home_region()
            if home_region == region_name:
                # This should never happen, as the instance should have been invoked in the home region
                # At least once, thus it should have runtime data
                raise ValueError(f"Instance {instance_name} has no runtime data in home region {home_region}, this should be impossible.")
            runtime_distribution = self._workflow_loader.get_runtime_distribution(instance_name, home_region)
            original_runtime_region_name = home_region

        # print(f"Instance Name: {instance_name}")
        # print(f"Original Region: {original_runtime_region_name}", f"Desired Region: {desired_runtime_region_name}")
        # print(f"Runtime Distribution: {runtime_distribution[:5]}")

        # Pick a random runtime from the distribution
        runtime: float = runtime_distribution[int(random.random() * (len(runtime_distribution) - 1))]
        return self._retrieve_runtimes_and_data_transfer(instance_name, original_runtime_region_name, desired_runtime_region_name, runtime, previous_cumulative_runtime)
    
    def _retrieve_runtimes_and_data_transfer(self, instance_name: str, original_runtime_region_name: str, desired_runtime_region_name: str, runtime: float, previous_cumulative_runtime: float) -> tuple[dict[str, Any], float]:
        # Retrieve the auxiliary_index_translation
        auxiliary_index_translation = self._workflow_loader.get_auxiliary_index_translation(instance_name)

        # print(f"Auxiliary Index Translation: {auxiliary_index_translation}")
        
        # Get the auxiliary data distribution of the instance in the given region
        # TODO: Cache this
        execution_auxiliary_data: list[list[float]] = self._workflow_loader.get_auxiliary_data_distribution(instance_name, original_runtime_region_name, runtime)

        # print(f"Execution Auxiliary Data: {execution_auxiliary_data}")

        # Pick a random auxiliary data from the distribution
        auxiliary_data: list[float] = execution_auxiliary_data[int(random.random() * (len(execution_auxiliary_data) - 1))]

        # Calculate the relative region performance
        # to original region
        relative_region_performance = 1.0
        if original_runtime_region_name != desired_runtime_region_name:
            # Get the relative performance of the region
            original_region_performance = self._performance_loader.get_relative_performance(original_runtime_region_name)
            desired_region_performance = self._performance_loader.get_relative_performance(desired_runtime_region_name)
            relative_region_performance = desired_region_performance / original_region_performance

        # Create the successor dictionary
        # Go through the auxiliary translation index and get every value other than data_transfer_during_execution_gb
        successors_runtime_data = {}
        for key, index in auxiliary_index_translation.items():
            if key != "data_transfer_during_execution_gb":
                successors_runtime_data[key] = previous_cumulative_runtime + auxiliary_data[index] * relative_region_performance

        # The key is the instance index of the successor
        # This need to be translated from index to instance name in the
        # input manager
        # The value is the cumulative runtime of when this
        # node invokes the successor
        return ({
            "current": previous_cumulative_runtime + (runtime * relative_region_performance),
            "successors": successors_runtime_data
        },
        auxiliary_data[auxiliary_index_translation["data_transfer_during_execution_gb"]]
        )
