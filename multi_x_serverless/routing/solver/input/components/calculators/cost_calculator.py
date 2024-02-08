from multi_x_serverless.routing.solver.input.components.calculator import InputCalculator
from multi_x_serverless.routing.solver.input.components.calculators.runtime_calculator import RuntimeCalculator
from multi_x_serverless.routing.solver.input.components.loaders.datacenter_loader import DatacenterLoader
from multi_x_serverless.routing.solver.input.components.loaders.workflow_loader import WorkflowLoader


class CostCalculator(InputCalculator):
    def __init__(
        self,
        datacenter_loader: DatacenterLoader,
        workflow_loader: WorkflowLoader,
        runtime_calculator: RuntimeCalculator,
    ) -> None:
        super().__init__()
        self._datacenter_loader: DatacenterLoader = datacenter_loader
        self._workflow_loader: WorkflowLoader = workflow_loader
        self._runtime_calculator: RuntimeCalculator = runtime_calculator

    # pylint: disable=no-else-raise
    def calculate_execution_cost(
        self, instance_name: str, region_name: str, consider_probabilistic_invocations: bool = False
    ) -> float:
        if consider_probabilistic_invocations:
            # TODO (#76): Implement probabilistic invocations
            raise NotImplementedError("Probabilistic invocations are not supported yet")
        else:
            return self._calculate_raw_execution_cost(instance_name, region_name, False)

    # pylint: disable=no-else-raise
    def calculate_transmission_cost(
        self,
        from_instance_name: str,
        to_instance_name: str,
        from_region_name: str,
        to_region_name: str,
        consider_probabilistic_invocations: bool = False,
    ) -> float:
        if consider_probabilistic_invocations:
            # TODO (#76): Implement probabilistic invocations
            raise NotImplementedError("Probabilistic invocations are not supported yet")
        else:
            return self._calculate_raw_transmission_cost(
                from_instance_name, to_instance_name, from_region_name, to_region_name
            )

    def _calculate_raw_execution_cost(
        self, instance_name: str, region_name: str, use_tail_latency: bool = False
    ) -> float:
        # Retrieve or format input information

        ## Get the runtime of the instance in the given region (s)
        runtime = self._runtime_calculator.calculate_raw_runtime(instance_name, region_name, use_tail_latency)

        ## Get the number of vCPUs and Memory of the instance
        provider, _ = region_name.split(":")
        vcpu = self._workflow_loader.get_vcpu(instance_name, provider)
        memory = self._workflow_loader.get_memory(instance_name, provider)
        architecture = self._workflow_loader.get_architecture(instance_name, provider)

        ## datacenter loader data
        compute_cost = self._datacenter_loader.get_compute_cost(region_name, architecture)
        invocation_cost = self._datacenter_loader.get_invocation_cost(region_name, architecture)

        # Compute cost in USD /  GB-seconds
        # Memory in MB, execution_time in seconds, vcpu in vcpu
        memory_gb = memory / 1024
        gbs = memory_gb * vcpu * runtime

        cost_from_compute = compute_cost * gbs

        # Invocation cost is simply cost per invocation
        cost_from_invocation = invocation_cost

        total_execution_cost = cost_from_compute + cost_from_invocation
        return total_execution_cost

    def _calculate_raw_transmission_cost(
        self, from_instance_name: str, to_instance_name: str, from_region_name: str, to_region_name: str
    ) -> float:
        # Retrieve or format input information

        ## Get the data transfer size from the workflow loader (In units of GB)
        transmission_size = self._workflow_loader.get_data_transfer_size(from_instance_name, to_instance_name)

        # Get the providers of the 2 instances
        from_provider, _ = from_region_name.split(":")
        to_provider, _ = to_region_name.split(":")

        # Determine if the transfer is intra-provider or inter-provider
        intra_provider_transfer = False
        if from_provider == to_provider:
            intra_provider_transfer = True

        # Get the cost of transmission
        transmission_cost_gb = self._datacenter_loader.get_transmission_cost(to_provider, intra_provider_transfer)

        # Both in units of gb
        transmission_cost = transmission_size * transmission_cost_gb

        return transmission_cost
