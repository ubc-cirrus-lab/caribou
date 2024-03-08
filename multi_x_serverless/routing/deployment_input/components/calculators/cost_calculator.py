import numpy as np

from multi_x_serverless.routing.deployment_input.components.calculator import InputCalculator
from multi_x_serverless.routing.deployment_input.components.calculators.runtime_calculator import RuntimeCalculator
from multi_x_serverless.routing.deployment_input.components.loaders.datacenter_loader import DatacenterLoader
from multi_x_serverless.routing.deployment_input.components.loaders.workflow_loader import WorkflowLoader


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
        
        # Conversion ratio cache
        self._execution_conversion_ratio_cache: dict[str, tuple[float, float]] = {}
        self._transmission_conversion_ratio_cache: dict[str, float] = {}

    def calculate_transmission_cost(self, from_instance_name: str, to_instance_name: str, from_region_name: str, to_region_name: str, data_transfer_size: float) -> float:
        transmission_cost_gb = self._get_transmission_conversion_ratio(from_instance_name, to_instance_name, from_region_name, to_region_name)
        return transmission_cost_gb * data_transfer_size

    def calculate_execution_cost(self, instance_name: str, region_name: str, execution_latency: float) -> float:
        cost_from_compute_s, invocation_cost = self._get_execution_conversion_ratio(instance_name, region_name)
        return cost_from_compute_s * execution_latency + invocation_cost

    def _get_transmission_conversion_ratio(self, from_instance_name: str, to_instance_name: str, from_region_name: str, to_region_name: str) -> float:
        # Check if the conversion ratio is in the cache
        key = from_instance_name + "_" + to_instance_name + "_" + from_region_name + "_" + to_region_name
        if key in self._transmission_conversion_ratio_cache:
            return self._transmission_conversion_ratio_cache[key]

        # Get the providers of the 2 instances
        from_provider, _ = from_region_name.split(":")
        to_provider, _ = to_region_name.split(":")

        # Determine if the transfer is intra-provider or inter-provider
        intra_provider_transfer: bool = False
        if from_provider == to_provider:
            intra_provider_transfer = True

        # Get the cost of transmission
        transmission_cost_gb: float = self._datacenter_loader.get_transmission_cost(
            to_region_name, intra_provider_transfer
        )

        # Add the conversion ratio to the cache
        self._transmission_conversion_ratio_cache[key] = transmission_cost_gb
        return self._transmission_conversion_ratio_cache[key]

    def _get_execution_conversion_ratio(self, instance_name: str, region_name: str) -> tuple[float, float]:
        # Check if the conversion ratio is in the cache
        key = instance_name + "_" + region_name
        if key in self._execution_conversion_ratio_cache:
            return self._execution_conversion_ratio_cache[key]
        
        # Get the number of vCPUs and Memory of the instance
        provider, _ = region_name.split(":")
        vcpu: float = self._workflow_loader.get_vcpu(instance_name, provider)
        memory: float = self._workflow_loader.get_memory(instance_name, provider)

        ## datacenter loader data
        architecture: str = self._workflow_loader.get_architecture(instance_name, provider)
        compute_cost: float = self._datacenter_loader.get_compute_cost(region_name, architecture)
        invocation_cost: float = self._datacenter_loader.get_invocation_cost(region_name, architecture)

        # Compute cost in USD /  GB-seconds
        # Memory in MB, execution_time in seconds, vcpu in vcpu
        memory_gb: float = memory / 1024
        gbs: np.ndarray = memory_gb * vcpu

        cost_from_compute_s: np.ndarray = compute_cost * gbs

        # Add the conversion ratio to the cache
        self._execution_conversion_ratio_cache[key] = (cost_from_compute_s, invocation_cost)
        return self._execution_conversion_ratio_cache[key]

    def calculate_execution_cost_distribution(self, instance_name: str, region_name: str) -> np.ndarray:
        # Get the runtime of the instance in the given region (s)
        runtime_distribution: np.ndarray = self._runtime_calculator.calculate_runtime_distribution(
            instance_name, region_name
        )

        # Get the number of vCPUs and Memory of the instance
        provider, _ = region_name.split(":")
        vcpu: float = self._workflow_loader.get_vcpu(instance_name, provider)
        memory: float = self._workflow_loader.get_memory(instance_name, provider)

        ## datacenter loader data
        architecture: str = self._workflow_loader.get_architecture(instance_name, provider)
        compute_cost: float = self._datacenter_loader.get_compute_cost(region_name, architecture)
        invocation_cost: float = self._datacenter_loader.get_invocation_cost(region_name, architecture)

        # Compute cost in USD /  GB-seconds
        # Memory in MB, execution_time in seconds, vcpu in vcpu
        memory_gb: float = memory / 1024
        gbs: np.ndarray = memory_gb * vcpu * runtime_distribution

        cost_from_compute: np.ndarray = compute_cost * gbs  # Element wise multiplication

        # Invocation cost is simply cost per invocation
        cost_from_invocation: float = invocation_cost

        total_execution_cost: np.ndarray = cost_from_compute + cost_from_invocation  # Element wise addition

        # Sort the array in place
        total_execution_cost.sort()

        return total_execution_cost

    def calculate_transmission_cost_distribution(
        self, from_instance_name: str, to_instance_name: str, from_region_name: str, to_region_name: str
    ) -> np.ndarray:
        if from_instance_name == "start_hop":
            return np.array([0.0])
        # Get the data transfer size from the workflow loader (In units of GB)
        transmission_size: np.ndarray = np.array(
            self._workflow_loader.get_data_transfer_size_distribution(from_instance_name, to_instance_name)
        )

        # Get the providers of the 2 instances
        from_provider, _ = from_region_name.split(":")
        to_provider, _ = to_region_name.split(":")

        # Determine if the transfer is intra-provider or inter-provider
        intra_provider_transfer: bool = False
        if from_provider == to_provider:
            intra_provider_transfer = True

        # Get the cost of transmission
        transmission_cost_gb: float = self._datacenter_loader.get_transmission_cost(
            to_region_name, intra_provider_transfer
        )

        # Both in units of gb
        transmission_cost: np.ndarray = transmission_size * transmission_cost_gb  # Element wise multiplication

        # Sort the array in place
        transmission_cost.sort()

        return transmission_cost
