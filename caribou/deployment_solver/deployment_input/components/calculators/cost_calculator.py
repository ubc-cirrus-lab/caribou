from caribou.deployment_solver.deployment_input.components.calculator import InputCalculator
from caribou.deployment_solver.deployment_input.components.calculators.runtime_calculator import RuntimeCalculator
from caribou.deployment_solver.deployment_input.components.loaders.datacenter_loader import DatacenterLoader
from caribou.deployment_solver.deployment_input.components.loaders.workflow_loader import WorkflowLoader


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

    def calculate_transmission_cost(
        self,
        from_region_name: str,
        to_region_name: str,
        data_transfer_size: float,
    ) -> float:
        if from_region_name == to_region_name:  # No egress cost from self transmission
            return 0.0

        transmission_cost_gb = self._get_transmission_conversion_ratio(from_region_name, to_region_name)
        return transmission_cost_gb * data_transfer_size

    def calculate_execution_cost(self, instance_name: str, region_name: str, execution_latency: float) -> float:
        cost_from_compute_s, invocation_cost = self._get_execution_conversion_ratio(instance_name, region_name)
        return cost_from_compute_s * execution_latency + invocation_cost

    def _get_transmission_conversion_ratio(self, from_region_name: str, to_region_name: str) -> float:
        # Check if the conversion ratio is in the cache
        key = from_region_name + "_" + to_region_name
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
        memory: float = self._workflow_loader.get_memory(instance_name, provider)

        ## datacenter loader data
        architecture: str = self._workflow_loader.get_architecture(instance_name, provider)
        compute_cost: float = self._datacenter_loader.get_compute_cost(region_name, architecture)
        invocation_cost: float = self._datacenter_loader.get_invocation_cost(region_name, architecture)

        # Compute cost in USD /  GB-seconds
        # Memory in MB, execution_time in seconds, vcpu in vcpu
        memory_gb: float = memory / 1024
        cost_from_compute_s: float = compute_cost * memory_gb  # IN USD / s

        # Add the conversion ratio to the cache
        self._execution_conversion_ratio_cache[key] = (cost_from_compute_s, invocation_cost)
        return self._execution_conversion_ratio_cache[key]

    def get_cache(self):
        return {
            'execution_conversion_ratio_cache': self._execution_conversion_ratio_cache,
            'transmission_conversion_ratio_cache': self._transmission_conversion_ratio_cache
        }

    def update_cache(self, cache: dict):
        self._execution_conversion_ratio_cache.update(cache['execution_conversion_ratio_cache'])
        self._transmission_conversion_ratio_cache.update(cache['transmission_conversion_ratio_cache'])

    @staticmethod
    def sync_caches(caches_list: list[dict]):
        synced_cache = {
            'execution_conversion_ratio_cache': {},
            'transmission_conversion_ratio_cache': {}
        }

        for cache in caches_list:
            for cache_key in synced_cache:
                synced_cache[cache_key].update(cache[cache_key])

        return synced_cache
