from typing import Optional

from multi_x_serverless.common.constants import (  # KWH_PER_GB_ESTIMATE,
    DFM,
    DFI,
)
from multi_x_serverless.routing.deployment_input.components.calculator import InputCalculator
from multi_x_serverless.routing.deployment_input.components.calculators.runtime_calculator import RuntimeCalculator
from multi_x_serverless.routing.deployment_input.components.loaders.carbon_loader import CarbonLoader
from multi_x_serverless.routing.deployment_input.components.loaders.datacenter_loader import DatacenterLoader
from multi_x_serverless.routing.deployment_input.components.loaders.workflow_loader import WorkflowLoader


class CarbonCalculator(InputCalculator):  # pylint: disable=too-many-instance-attributes
    def __init__(
        self,
        carbon_loader: CarbonLoader,
        datacenter_loader: DatacenterLoader,
        workflow_loader: WorkflowLoader,
        runtime_calculator: RuntimeCalculator,
        consider_cfe: bool = False,
    ) -> None:
        super().__init__()
        self._carbon_loader: CarbonLoader = carbon_loader
        self._datacenter_loader: DatacenterLoader = datacenter_loader
        self._workflow_loader: WorkflowLoader = workflow_loader
        self._runtime_calculator: RuntimeCalculator = runtime_calculator
        self._consider_cfe: bool = consider_cfe

        # Conversion ratio cache
        self._execution_conversion_ratio_cache: dict[str, tuple[float, float, float]] = {}
        self._transmission_conversion_ratio_cache: dict[str, float, float] = {}

        # Carbon setting - hourly or average policy
        self._hourly_carbon_setting: Optional[str] = None  # None indicates the default setting -> Average everything

    def alter_carbon_setting(self, carbon_setting: Optional[str]) -> None:
        self._hourly_carbon_setting = carbon_setting

        # Clear the cache
        self._execution_conversion_ratio_cache = {}
        self._transmission_conversion_ratio_cache = {}

    def calculate_execution_carbon(self, instance_name: str, region_name: str, execution_latency: float) -> float:
        compute_factor, memory_factor, power_factor = self._get_execution_conversion_ratio(instance_name, region_name)

        cloud_provider_usage_kwh = execution_latency * (compute_factor + memory_factor)

        return cloud_provider_usage_kwh * power_factor

    def _get_execution_conversion_ratio(self, instance_name: str, region_name: str) -> tuple[float, float, float]:
        # Check if the conversion ratio is in the cache
        cache_key = f"{instance_name}_{region_name}"
        if cache_key in self._execution_conversion_ratio_cache:
            return self._execution_conversion_ratio_cache[cache_key]

        # datacenter loader data
        ## Get the average power consumption of the instance in the given region (kw_compute)
        average_cpu_power: float = self._datacenter_loader.get_average_cpu_power(region_name)

        ## Get the average power consumption of the instance in the given region (kw_GB)
        average_memory_power: float = self._datacenter_loader.get_average_memory_power(region_name)

        ## Get the carbon free energy of the grid in the given region
        cfe: float = 0.0
        if self._consider_cfe:
            cfe = self._datacenter_loader.get_cfe(region_name)

        ## Get the power usage effectiveness of the datacenter in the given region
        pue: float = self._datacenter_loader.get_pue(region_name)

        ## Get the carbon intensity of the grid in the given region (gCO2e/kWh)
        grid_co2e: float = self._carbon_loader.get_grid_carbon_intensity(region_name, self._hourly_carbon_setting)

        ## Get the number of vCPUs and Memory of the instance
        provider, _ = region_name.split(":")  # Get the provider from the region name
        vcpu: float = self._workflow_loader.get_vcpu(instance_name, provider)
        memory: float = self._workflow_loader.get_memory(instance_name, provider)

        # Covert memory in MB to GB
        memory = memory / 1024

        compute_factor = average_cpu_power * vcpu / 3600
        memory_factor = average_memory_power * memory / 3600
        power_factor = (1 - cfe) * pue * grid_co2e

        # Add the conversion ratio to the cache
        self._execution_conversion_ratio_cache[cache_key] = (compute_factor, memory_factor, power_factor)
        return self._execution_conversion_ratio_cache[cache_key]

    def calculate_transmission_carbon(
        self, from_region_name: str, to_region_name: str, data_transfer_size: float, transmission_latency: float
    ) -> float:
        distance_factor_distance = self._get_transmission_conversion_ratio(
            from_region_name, to_region_name
        )
        return data_transfer_size * distance_factor_distance

    def _get_transmission_conversion_ratio(self, from_region_name: str, to_region_name: str) -> float:
        # Check if the conversion ratio is in the cache
        cache_key = f"{from_region_name}_{to_region_name}"
        if cache_key in self._transmission_conversion_ratio_cache:
            return self._transmission_conversion_ratio_cache[cache_key]

        # Get the distance in KM between the two regions
        distance = self._carbon_loader.get_transmission_distance(from_region_name, to_region_name)
        if distance < 0:
            raise ValueError(f"Distance between {from_region_name} and {to_region_name} is not available")

        ## Get the carbon intensity of the grid in the given region (gCO2e/kWh)
        from_region_carbon_intensity: float = self._carbon_loader.get_grid_carbon_intensity(
            from_region_name, self._hourly_carbon_setting
        )
        to_region_carbon_intensity: float = self._carbon_loader.get_grid_carbon_intensity(
            to_region_name, self._hourly_carbon_setting
        )

        transmission_carbon_intensity = (from_region_carbon_intensity + to_region_carbon_intensity) / 2  # gCo2eq/kWh

        distance_factor_distance = transmission_carbon_intensity * (DFM * distance + DFI)  # gCo2eq/GB

        # Add the conversion ratio to the cache
        self._transmission_conversion_ratio_cache[cache_key] = distance_factor_distance
        return self._transmission_conversion_ratio_cache[cache_key]
