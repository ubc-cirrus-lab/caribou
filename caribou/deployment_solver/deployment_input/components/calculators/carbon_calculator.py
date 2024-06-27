from typing import Optional

from caribou.deployment_solver.deployment_input.components.calculator import InputCalculator
from caribou.deployment_solver.deployment_input.components.calculators.runtime_calculator import RuntimeCalculator
from caribou.deployment_solver.deployment_input.components.loaders.carbon_loader import CarbonLoader
from caribou.deployment_solver.deployment_input.components.loaders.datacenter_loader import DatacenterLoader
from caribou.deployment_solver.deployment_input.components.loaders.workflow_loader import WorkflowLoader


class CarbonCalculator(InputCalculator):  # pylint: disable=too-many-instance-attributes
    def __init__(
        self,
        carbon_loader: CarbonLoader,
        datacenter_loader: DatacenterLoader,
        workflow_loader: WorkflowLoader,
        energy_factor_of_transmission: float = 0.005,
        consider_home_region_for_transmission: bool = True,
        consider_cfe: bool = False,
    ) -> None:
        super().__init__()
        self._carbon_loader: CarbonLoader = carbon_loader
        self._datacenter_loader: DatacenterLoader = datacenter_loader
        self._workflow_loader: WorkflowLoader = workflow_loader
        # self._runtime_calculator: RuntimeCalculator = runtime_calculator
        self._consider_cfe: bool = consider_cfe

        # Conversion ratio cache
        self._execution_conversion_ratio_cache: dict[str, tuple[float, float, float]] = {}
        self._transmission_conversion_ratio_cache: dict[str, float] = {}

        # Carbon setting - hourly or average policy
        self._hourly_carbon_setting: Optional[str] = None  # None indicates the default setting -> Average everything

        # Energy factor and if considering home region for transmission carbon calculation
        self._energy_factor_of_transmission: float = energy_factor_of_transmission

        # This denotes if we should consider the home region for transmission carbon calculation
        # Meaning that if this is true, we consider data transfer within the same region as incrring
        # transmission carbon.
        self._consider_home_region_for_transmission: bool = consider_home_region_for_transmission

    def alter_carbon_setting(self, carbon_setting: Optional[str]) -> None:
        self._hourly_carbon_setting = carbon_setting

        # Clear the cache
        self._execution_conversion_ratio_cache = {}
        self._transmission_conversion_ratio_cache = {}

    def calculate_instance_carbon(
            self,
            runtime: float,
            instance_name: str,
            region_name: str,
            data_input_sizes: dict[str, float],
            data_output_sizes: dict[str, float],
            data_transfer_during_execution: float,
            is_invoked: bool) -> tuple[float, float]:
        execution_carbon = 0.0
        transmission_carbon = 0.0

        # print(f"instance_name: {instance_name}, at region: {region_name}")

        # If the function is actually invoked
        if is_invoked:
            # Calculate the carbon from running the execution
            execution_carbon += self._calculate_execution_carbon(instance_name, region_name, runtime)

        # Even if the function is not invoked, we model
        # Each node as an abstract instance to consider
        # data transfer carbon
        transmission_carbon += self._calculate_data_transfer_carbon(region_name, data_input_sizes, data_output_sizes, data_transfer_during_execution)

        return execution_carbon, transmission_carbon
    
    def _calculate_data_transfer_carbon(self, current_region_name: str,
                                        data_input_sizes: dict[str, float],
                                        data_output_sizes: dict[str, float],
                                        data_transfer_during_execution: float) -> float:
        total_transmission_carbon: float = 0.0
        current_region_carbon_intensity = self._carbon_loader.get_grid_carbon_intensity(
            current_region_name, self._hourly_carbon_setting
        )

        # Make a new dictionary where the key is other region name
        # and the value is the data transfer size (If data_input sizes
        # and data_output sizes have the same key, we add the values)
        data_transfer_sizes = {k: data_input_sizes.get(k, 0) + data_output_sizes.get(k, 0) for k in set(data_input_sizes) | set(data_output_sizes)}
        
        # print(f'data_input_sizes: {data_input_sizes}')
        # print(f'data_output_sizes: {data_output_sizes}')
        # print(f'data_transfer_sizes: {data_transfer_sizes}\n')

        # Deal with carbon that we can track
        for other_region_name, data_transfer_gb in data_transfer_sizes.items():
            # If consider_home_region_for_transmission is true,
            # then we consider there are transmission carbon EVEN for
            # data transfer within the same region.
            # Otherwise, we skip the data transfer within the same region
            if current_region_name == other_region_name:
                if not self._consider_home_region_for_transmission:
                    continue

            carbon_intensity_of_transmission_route = current_region_carbon_intensity
            if other_region_name is not None:
                other_region_carbon_intensity: float = self._carbon_loader.get_grid_carbon_intensity(
                    other_region_name, self._hourly_carbon_setting
                )

                # Calculate the carbon from data transfer of the carbon intensity of the route
                # TODO: Look into changing this if its not appropriate
                carbon_intensity_of_transmission_route = (other_region_carbon_intensity + current_region_carbon_intensity) / 2
            
            total_transmission_carbon += data_transfer_gb * self._energy_factor_of_transmission * carbon_intensity_of_transmission_route

        # Calculate the carbon from data transfer
        # Of data that we CANNOT track represented by data_transfer_during_execution
        # Right now we are just assuming they are from the home region
        # if not ((current_region_name == self._workflow_loader.get_home_region()) and not ):
        if (self._consider_home_region_for_transmission or (current_region_name != self._workflow_loader.get_home_region())):
            # Perhaps use global carbon intensity
            total_transmission_carbon += data_transfer_during_execution * self._energy_factor_of_transmission * current_region_carbon_intensity

        return total_transmission_carbon

    def _calculate_execution_carbon(self, instance_name: str, region_name: str, execution_latency: float) -> float:
        # Calculate the carbon from running the execution (solely for cpu and memory)
        compute_factor, memory_factor, power_factor = self._get_execution_conversion_ratio(instance_name, region_name)
        cloud_provider_usage_kwh = execution_latency * (compute_factor + memory_factor)
        execution_carbon = cloud_provider_usage_kwh * power_factor

        return execution_carbon
    
    def _get_execution_conversion_ratio(self, instance_name: str, region_name: str) -> tuple[float, float, float]:
        # Check if the conversion ratio is in the cache
        cache_key = f"{instance_name}_{region_name}"
        if cache_key in self._execution_conversion_ratio_cache:
            return self._execution_conversion_ratio_cache[cache_key]

        # datacenter loader data
        ## Get the average power consumption of the instance in the given region (kw_compute)
        # average_cpu_power: float = self._datacenter_loader.get_average_cpu_power(region_name)

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

        # Get the min/max cpu power (In units of kWh)
        min_cpu_power: float = self._datacenter_loader.get_min_cpu_power(region_name)
        max_cpu_power: float = self._datacenter_loader.get_max_cpu_power(region_name)

        # Covert memory in MB to GB
        memory = memory / 1024

        # Get the average cpu utilization of the instance
        utilization = self._workflow_loader.get_average_cpu_utilization(instance_name)
        # average_cpu_power = (0.74 + utilization * (3.5 - 0.74)) / 1000
        average_cpu_power = min_cpu_power + utilization * (max_cpu_power - min_cpu_power)

        compute_factor = average_cpu_power * vcpu / 3600
        memory_factor = average_memory_power * memory / 3600
        power_factor = (1 - cfe) * pue * grid_co2e

        # Add the conversion ratio to the cache
        self._execution_conversion_ratio_cache[cache_key] = (compute_factor, memory_factor, power_factor)
        return self._execution_conversion_ratio_cache[cache_key]
