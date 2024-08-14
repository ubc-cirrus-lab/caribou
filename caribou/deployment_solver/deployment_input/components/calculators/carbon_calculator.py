from typing import Optional

from caribou.common.constants import AVERAGE_USA_CARBON_INTENSITY
from caribou.deployment_solver.deployment_input.components.calculator import InputCalculator
from caribou.deployment_solver.deployment_input.components.loaders.carbon_loader import CarbonLoader
from caribou.deployment_solver.deployment_input.components.loaders.datacenter_loader import DatacenterLoader
from caribou.deployment_solver.deployment_input.components.loaders.workflow_loader import WorkflowLoader


class CarbonCalculator(InputCalculator):  # pylint: disable=too-many-instance-attributes
    def __init__(
        self,
        carbon_loader: CarbonLoader,
        datacenter_loader: DatacenterLoader,
        workflow_loader: WorkflowLoader,
        energy_factor_of_transmission: float = 0.001,
        carbon_free_intra_region_transmission: bool = False,
        carbon_free_dt_during_execution_at_home_region: bool = False,
        consider_cfe: bool = False,
    ) -> None:
        super().__init__()
        self._carbon_loader: CarbonLoader = carbon_loader
        self._datacenter_loader: DatacenterLoader = datacenter_loader
        self._workflow_loader: WorkflowLoader = workflow_loader
        self._consider_cfe: bool = consider_cfe

        # Conversion ratio cache
        self._execution_conversion_ratio_cache: dict[str, tuple[float, float, float]] = {}
        self._transmission_conversion_ratio_cache: dict[str, float] = {}

        # Carbon setting - hourly or average policy
        self._hourly_carbon_setting: Optional[str] = None  # None indicates the default setting -> Average everything

        # Energy factor and if considering home region for transmission carbon calculation
        self._energy_factor_of_transmission: float = energy_factor_of_transmission

        # This denotes if we should consider the intra region for transmission carbon calculation
        # Meaning that if this is true, we consider data transfer within the same region as incrring
        # transmission carbon.
        self._carbon_free_intra_region_transmission: bool = carbon_free_intra_region_transmission

        # Consider the case of data transfer during execution being free at home region
        # of the user workflow. (Basically making the assumption that functions deployed
        # at user specified home region will not incurr carbon from data transfer )
        self._carbon_free_dt_during_execution_at_home_region: bool = carbon_free_dt_during_execution_at_home_region

    def alter_carbon_setting(self, carbon_setting: Optional[str]) -> None:
        self._hourly_carbon_setting = carbon_setting

        # Clear the cache
        self._execution_conversion_ratio_cache = {}
        self._transmission_conversion_ratio_cache = {}

    def calculate_virtual_start_instance_carbon(
        self,
        data_input_sizes: dict[Optional[str], float],
        data_output_sizes: dict[Optional[str], float],
    ) -> float:
        transmission_carbon = 0.0

        # Even if the function is not invoked, we model
        # Each node as an abstract instance to consider
        # data transfer carbon
        transmission_carbon += self._calculate_data_transfer_carbon(None, data_input_sizes, data_output_sizes, 0.0)

        return transmission_carbon

    def calculate_instance_carbon(
        self,
        execution_time: float,
        instance_name: str,
        region_name: str,
        data_input_sizes: dict[Optional[str], float],
        data_output_sizes: dict[Optional[str], float],
        data_transfer_during_execution: float,
        is_invoked: bool,
        is_redirector: bool,
    ) -> tuple[float, float]:
        execution_carbon = 0.0
        transmission_carbon = 0.0

        # print(f"instance_name: {instance_name}, at region: {region_name}")

        # If the function is actually invoked
        if is_invoked:
            # Calculate the carbon from running the execution
            execution_carbon += self._calculate_execution_carbon(
                instance_name, region_name, execution_time, is_redirector
            )

        # Even if the function is not invoked, we model
        # Each node as an abstract instance to consider
        # data transfer carbon
        transmission_carbon += self._calculate_data_transfer_carbon(
            region_name, data_input_sizes, data_output_sizes, data_transfer_during_execution
        )

        return execution_carbon, transmission_carbon

    def _calculate_data_transfer_carbon(
        self,
        current_region_name: Optional[str],
        data_input_sizes: dict[Optional[str], float],
        data_output_sizes: dict[Optional[str], float],  # pylint: disable=unused-argument
        data_transfer_during_execution: float,
    ) -> float:
        total_transmission_carbon: float = 0.0
        average_carbon_intensity_of_usa: float = AVERAGE_USA_CARBON_INTENSITY

        # Since the energy factor of transmission denotes the energy consumption
        # of the data transfer from and to destination, we do not want to double count.
        # Thus we can simply take a look at the data_input_sizes and ignore the data_output_sizes.
        data_transfer_accounted_by_wrapper = data_input_sizes
        for from_region_name, data_transfer_gb in data_transfer_accounted_by_wrapper.items():
            transmission_network_carbon_intensity = average_carbon_intensity_of_usa
            # If consider_home_region_for_transmission is true,
            # then we consider there are transmission carbon EVEN for
            # data transfer within the same region.
            # Otherwise, we skip the data transfer within the same region
            if from_region_name == current_region_name:
                # If its intra region transmission, and if we
                # want to consider it as free, then we skip it.
                if self._carbon_free_intra_region_transmission:
                    continue

                if current_region_name is not None:
                    # Get the carbon intensity of the region (if known)
                    # (If data transfer is within the same region)
                    # Otherwise it will be inter-region data transfer,
                    transmission_network_carbon_intensity = self._carbon_loader.get_grid_carbon_intensity(
                        current_region_name, self._hourly_carbon_setting
                    )
            elif from_region_name is not None and current_region_name is not None:
                # If we know the source and destination regions, we can get the carbon intensity
                # of the transmission network between the two regions.
                transmission_network_carbon_intensity = self._get_network_carbon_intensity_of_route_between_two_regions(
                    from_region_name, current_region_name
                )

            total_transmission_carbon += (
                data_transfer_gb * self._energy_factor_of_transmission * transmission_network_carbon_intensity
            )

        # Calculate the carbon from data transfer
        # Of data that we CANNOT track represented by data_transfer_during_execution.
        # There are no way to tell where the data is coming from
        # This may come from the data transfer of user code during execution OR
        # From Lambda runtimes or some AWS internal data transfer.
        home_region = self._workflow_loader.get_home_region()
        current_region_is_home_region = current_region_name == home_region

        # We assume that half of the data transfer is from the home region
        # and the other half is from the average carbon intensity of the USA.
        home_region_dtde = internet_dtde = data_transfer_during_execution / 2

        # # For scenerio where DTDE is only from the home region
        # home_region_dtde = data_transfer_during_execution
        # internet_dtde = 0.0

        # If the data transfer is from the internet, we use the average carbon intensity of the USA
        # And it is always consider inter-region data transfer. (So always apply)
        total_transmission_carbon += (
            internet_dtde * self._energy_factor_of_transmission * average_carbon_intensity_of_usa
        )

        # If the data transfer is from the home region, we use the carbon intensity of the home region
        # And it is always consider intra-region data transfer. (May or may not apply)
        if not self._carbon_free_dt_during_execution_at_home_region or not current_region_is_home_region:
            transmission_network_carbon_intensity = average_carbon_intensity_of_usa
            if current_region_name is not None:
                transmission_network_carbon_intensity = self._get_network_carbon_intensity_of_route_between_two_regions(
                    home_region, current_region_name
                )

            total_transmission_carbon += (
                home_region_dtde * self._energy_factor_of_transmission * transmission_network_carbon_intensity
            )

        # if not self._carbon_free_dt_during_execution_at_home_region or not current_region_is_home_region:
        #     transmission_network_carbon_intensity = average_carbon_intensity_of_usa
        #     # if current_region_is_home_region and current_region_name is not None:
        #     #     # Here we make the assumption that the user code accesses data from the home region
        #     #     # thus the grid carbon intensity will be the same as the home region if it is at the home region.
        #     #     # Otherwise, we use the average carbon intensity of the USA.
        #     #     transmission_network_carbon_intensity = self._carbon_loader.get_grid_carbon_intensity(
        #     #         current_region_name, self._hourly_carbon_setting
        #     #     )
        #     if current_region_name is not None:
        #         # There are no way to tell where the data is coming from
        #         # But lets make the assumption that at least 50% of the data transfer
        #         # is from the home region, and the other 50% can be from anywhere in the USA.
        #         transmission_carbon_route_from_home_region =
        #     self._get_network_carbon_intensity_of_route_between_two_regions(
        #             home_region, current_region_name
        #         )

        #         # transmission_network_carbon_intensity = transmission_carbon_route_from_home_region

        #         # Assume that the data transfer carbon is 50% from the home region and 50%
        #           from the average carbon intensity of the USA
        #         transmission_network_carbon_intensity = (
        #             0.5 * transmission_carbon_route_from_home_region
        #             + 0.5 * average_carbon_intensity_of_usa
        #         )

        #     total_transmission_carbon += (
        #         data_transfer_during_execution
        #         * self._energy_factor_of_transmission
        #         * transmission_network_carbon_intensity
        #     )

        return total_transmission_carbon

    def _get_network_carbon_intensity_of_route_between_two_regions(self, region_one: str, region_two: str) -> float:
        if region_one == region_two and region_one is not None:
            region_one_carbon_intensity = self._carbon_loader.get_grid_carbon_intensity(
                region_one, self._hourly_carbon_setting
            )
            return region_one_carbon_intensity
        # else:
        #     return AVERAGE_USA_CARBON_INTENSITY

        # Get the carbon intensity of the route betweem two regions.
        # We can estimate it as the average carbon intensity of the grid
        # between the two regions. (No order is assumed)
        # If we have a better model, we can replace this with that.
        region_one_carbon_intensity = self._carbon_loader.get_grid_carbon_intensity(
            region_one, self._hourly_carbon_setting
        )
        region_two_carbon_intensity = self._carbon_loader.get_grid_carbon_intensity(
            region_two, self._hourly_carbon_setting
        )
        transmission_network_carbon_intensity = (region_one_carbon_intensity + region_two_carbon_intensity) / 2

        return transmission_network_carbon_intensity

    def _calculate_execution_carbon(
        self, instance_name: str, region_name: str, execution_latency: float, is_redirector: bool
    ) -> float:
        # Calculate the carbon from running the execution (solely for cpu and memory)
        compute_factor, memory_factor, power_factor = self._get_execution_conversion_ratio(
            instance_name, region_name, is_redirector
        )
        cloud_provider_usage_kwh = execution_latency * (compute_factor + memory_factor)
        execution_carbon = cloud_provider_usage_kwh * power_factor

        return execution_carbon

    def _get_execution_conversion_ratio(
        self, instance_name: str, region_name: str, is_redirector: bool
    ) -> tuple[float, float, float]:
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
        utilization = self._workflow_loader.get_average_cpu_utilization(instance_name, region_name, is_redirector)
        # average_cpu_power = (0.74 + utilization * (3.5 - 0.74)) / 1000
        average_cpu_power = min_cpu_power + utilization * (max_cpu_power - min_cpu_power)

        compute_factor = average_cpu_power * vcpu / 3600
        memory_factor = average_memory_power * memory / 3600
        power_factor = (1 - cfe) * pue * grid_co2e

        # Add the conversion ratio to the cache
        self._execution_conversion_ratio_cache[cache_key] = (compute_factor, memory_factor, power_factor)
        return self._execution_conversion_ratio_cache[cache_key]
