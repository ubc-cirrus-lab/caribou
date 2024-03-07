import numpy as np

from multi_x_serverless.routing.deployment_input.components.calculator import InputCalculator
from multi_x_serverless.routing.deployment_input.components.loaders.performance_loader import PerformanceLoader
from multi_x_serverless.routing.deployment_input.components.loaders.workflow_loader import WorkflowLoader


class RuntimeCalculator(InputCalculator):
    def __init__(self, performance_loader: PerformanceLoader, workflow_loader: WorkflowLoader) -> None:
        super().__init__()
        self._performance_loader: PerformanceLoader = performance_loader
        self._workflow_loader: WorkflowLoader = workflow_loader

    def calculate_runtime_distribution(self, instance_name: str, region_name: str) -> np.ndarray:
        raw_runtime_distribution: list[float] = self._workflow_loader.get_runtime_distribution(
            instance_name, region_name
        )

        # If the runtime is not found, then we simply default to home region runtime
        if len(raw_runtime_distribution) == 0:
            home_region = self._workflow_loader.get_home_region()

            # Currently we do not consider relative performance such that we
            # Simply assume it runs the same in all regions to its home region
            # At least for bootstrapping purposes
            raw_runtime_distribution = self._workflow_loader.get_runtime_distribution(instance_name, home_region)

            # It is possible for a workflow to have no runtime distribution for a specific instance
            # This happens if the instance was never invoked in the workflow so far
            # such that they have no runtime data, in this case we assume the runtime is 0
            if len(raw_runtime_distribution) == 0:
                raw_runtime_distribution.append(0.0)

        # Now we convert the list to a numpy array an return it
        runtime_distribution = np.array(raw_runtime_distribution)

        # Sort the array in place
        runtime_distribution.sort()

        return runtime_distribution

    def calculate_latency_distribution(
        self, from_instance_name: str, to_instance_name: str, from_region_name: str, to_region_name: str
    ) -> np.ndarray:
        raw_latency_distribution: list[float] = self._workflow_loader.get_latency_distribution(
            from_instance_name, to_instance_name, from_region_name, to_region_name
        )

        # If the latency is not found, then we need to use the performance loader to estimate the relative latency
        if len(raw_latency_distribution) == 0:
            # TODO (#166): Potentially we can do something here? Take the home region latency?
            if from_instance_name == "start_hop":
                return np.array([0.0])
            # Currently our performance loader does not consider data transfer size for latency
            raw_latency_distribution = self._performance_loader.get_transmission_latency_distribution(
                from_region_name, to_region_name
            )

        # Now we convert the list to a numpy array an return it
        latency_distribution = np.array(raw_latency_distribution)

        # Sort the array in place
        latency_distribution.sort()

        return latency_distribution
