import random
from typing import Optional

from multi_x_serverless.routing.deployment_input.components.calculator import InputCalculator
from multi_x_serverless.routing.deployment_input.components.loaders.performance_loader import PerformanceLoader
from multi_x_serverless.routing.deployment_input.components.loaders.workflow_loader import WorkflowLoader


class RuntimeCalculator(InputCalculator):
    def __init__(self, performance_loader: PerformanceLoader, workflow_loader: WorkflowLoader) -> None:
        super().__init__()
        self._performance_loader: PerformanceLoader = performance_loader
        self._workflow_loader: WorkflowLoader = workflow_loader
        self._workflow_loader._performance_loader = performance_loader

    def get_transmission_size_distribution(
        self,
        from_instance_name: Optional[str],
        to_instance_name: str,
        from_region_name: str,
        to_region_name: str,
    ) -> list[float]:
        # Get the data transfer size distribution
        if from_instance_name:
            transmission_size_distribution = self._workflow_loader.get_data_transfer_size_distribution(
                from_instance_name, to_instance_name, from_region_name, to_region_name
            )
        else:
            home_region_name = from_region_name
            transmission_size_distribution = self._workflow_loader.get_start_hop_size_distribution(
                home_region_name, to_region_name
            )
        return transmission_size_distribution

    def get_transmission_latency_distribution(
        self,
        from_instance_name: Optional[str],
        to_instance_name: str,
        from_region_name: str,
        to_region_name: str,
        data_transfer_size: Optional[float],
    ) -> list[float]:
        if data_transfer_size is not None:
            if from_instance_name:
                # Not for start hop
                # Get the data transfer size distribution
                transmission_latency_distribution = self._workflow_loader.get_latency_distribution(
                    from_instance_name, to_instance_name, from_region_name, to_region_name, data_transfer_size
                )
            else:
                home_region_name = from_region_name
                # No size information, we default to performance loader
                transmission_latency_distribution = self._workflow_loader.get_start_hop_latency_distribution(
                    home_region_name, to_region_name, data_transfer_size
                )
        else:
            # No size information, we default to performance loader
            transmission_latency_distribution = self._performance_loader.get_transmission_latency_distribution(
                from_region_name, to_region_name
            )

            # Since this will underestimate the latency, we multiply by the
            # underestimation factor retrieved from home region
            home_region_name = self._workflow_loader.get_home_region()
            home_region_latency_distribution_performance = (
                self._performance_loader.get_transmission_latency_distribution(home_region_name, home_region_name)
            )

            # Select a random latency from the distribution
            home_region_latency_performance = home_region_latency_distribution_performance[
                int(random.random() * (len(home_region_latency_distribution_performance) - 1))
            ]

            assert (
                from_instance_name is not None
            ), "From instance name must be provided for underestimation factor, something went wrong."

            home_region_transmission_size_distribution = self._workflow_loader.get_data_transfer_size_distribution(
                from_instance_name, to_instance_name, home_region_name, home_region_name
            )

            # Select a random size from the distribution
            home_region_transmission_size = home_region_transmission_size_distribution[
                int(random.random() * (len(home_region_transmission_size_distribution) - 1))
            ]

            home_region_latency_distribution_measured = self._workflow_loader.get_latency_distribution(
                from_instance_name, to_instance_name, home_region_name, home_region_name, home_region_transmission_size
            )

            home_region_latency_measured = home_region_latency_distribution_measured[
                int(random.random() * (len(home_region_latency_distribution_measured) - 1))
            ]

            # Calculate the underestimation factor
            underestimation_factor = home_region_latency_measured / home_region_latency_performance

            # Apply the underestimation factor
            transmission_latency_distribution = [
                latency * underestimation_factor for latency in transmission_latency_distribution
            ]

        return transmission_latency_distribution

    def calculate_runtime_distribution(self, instance_name: str, region_name: str) -> list[float]:
        runtime_distribution: list[float] = self._workflow_loader.get_runtime_distribution(instance_name, region_name)

        # If the runtime is not found, then we simply default to home region runtime
        if len(runtime_distribution) == 0:
            home_region = self._workflow_loader.get_home_region()

            # Currently we do not consider relative performance such that we
            # Simply assume it runs the same in all regions to its home region
            # At least for bootstrapping purposes
            runtime_distribution = self._workflow_loader.get_runtime_distribution(instance_name, home_region)

            # It is possible for a workflow to have no runtime distribution for a specific instance
            # This happens if the instance was never invoked in the workflow so far
            # such that they have no runtime data, in this case we assume the runtime is 0
            if len(runtime_distribution) == 0:
                runtime_distribution.append(0.0)

        return runtime_distribution
