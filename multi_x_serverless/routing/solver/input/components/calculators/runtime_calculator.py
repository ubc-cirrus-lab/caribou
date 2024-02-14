from multi_x_serverless.routing.models.dag import DAG
from multi_x_serverless.routing.solver.input.components.calculator import InputCalculator
from multi_x_serverless.routing.solver.input.components.loaders.performance_loader import PerformanceLoader
from multi_x_serverless.routing.solver.input.components.loaders.workflow_loader import WorkflowLoader


class RuntimeCalculator(InputCalculator):
    _dag: DAG

    def __init__(self, performance_loader: PerformanceLoader, workflow_loader: WorkflowLoader) -> None:
        super().__init__()
        self._performance_loader: PerformanceLoader = performance_loader
        self._workflow_loader: WorkflowLoader = workflow_loader

    def calculate_runtime(
        self, instance_name: str, region_name: str, consider_probabilistic_invocations: bool = False
    ) -> float:
        if consider_probabilistic_invocations:
            # TODO (#76): Implement probabilistic invocations
            raise NotImplementedError("Probabilistic invocations are not supported yet")

        return self.calculate_raw_runtime(instance_name, region_name, False)

    def calculate_latency(
        self,
        from_instance_name: str,
        to_instance_name: str,
        from_region_name: str,
        to_region_name: str,
        consider_probabilistic_invocations: bool = False,
    ) -> float:  # pylint: disable=no-else-raise
        if consider_probabilistic_invocations:
            # TODO (#76): Implement probabilistic invocations
            raise NotImplementedError("Probabilistic invocations are not supported yet")

        return self.calculate_raw_latency(from_instance_name, to_instance_name, from_region_name, to_region_name, False)

    def calculate_raw_runtime(self, instance_name: str, region_name: str, use_tail_runtime: bool = False) -> float:
        runtime = self._workflow_loader.get_runtime(instance_name, region_name, use_tail_runtime)

        # If the runtime is not found, then we need to use the performance loader to estimate the relative runtime
        if runtime < 0:
            favourite_home_region = self._workflow_loader.get_favourite_region(instance_name)
            if favourite_home_region is not None:
                # Get the performance of the instance in the favourite home region
                favourite_region_runtime = self._workflow_loader.get_favourite_region_runtime(instance_name)
                favourite_region_relative_performance = self._performance_loader.get_relative_performance(
                    favourite_home_region
                )

                desired_region_relative_performance = self._performance_loader.get_relative_performance(region_name)

                # Calculate the estimated runtime in the desired region
                runtime = favourite_region_runtime * (
                    desired_region_relative_performance / favourite_region_relative_performance
                )
            else:
                runtime = 0  # Instance was never invoked, so we assume it has no runtime

        return runtime

    def calculate_raw_latency(
        self,
        from_instance_name: str,
        to_instance_name: str,
        from_region_name: str,
        to_region_name: str,
        use_tail_latency: bool = False,
    ) -> float:
        latency = self._workflow_loader.get_latency(
            from_instance_name, to_instance_name, from_region_name, to_region_name, use_tail_latency
        )

        # If the latency is not found, then we need to use the performance loader to estimate the relative latency
        if latency < 0:
            # Get the data transfer size from the workflow loader
            data_transfer_size = self._workflow_loader.get_data_transfer_size(from_instance_name, to_instance_name)

            latency = self._performance_loader.get_transmission_latency(
                from_region_name, to_region_name, data_transfer_size, use_tail_latency
            )

        return latency
