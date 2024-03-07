import numpy as np

from multi_x_serverless.common.constants import (
    CARBON_TRANSMISSION_CARBON_METHOD,
    KWH_PER_GB_ESTIMATE,
    KWH_PER_KM_GB_ESTIMATE,
    KWH_PER_S_GB_ESTIMATE,
)
from multi_x_serverless.routing.deployment_input.components.calculator import InputCalculator
from multi_x_serverless.routing.deployment_input.components.calculators.runtime_calculator import RuntimeCalculator
from multi_x_serverless.routing.deployment_input.components.loaders.carbon_loader import CarbonLoader
from multi_x_serverless.routing.deployment_input.components.loaders.datacenter_loader import DatacenterLoader
from multi_x_serverless.routing.deployment_input.components.loaders.workflow_loader import WorkflowLoader


class CarbonCalculator(InputCalculator):
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

    def calculate_execution_carbon_distribution(self, instance_name: str, region_name: str) -> np.ndarray:
        # Get the runtime of the instance in the given region (s)
        runtime_distributions: np.ndarray = self._runtime_calculator.calculate_runtime_distribution(
            instance_name, region_name
        )

        # datacenter loader data
        ## Get the average power consumption of the instance in the given region (kw_compute)
        average_cpu_power: float = self._datacenter_loader.get_average_cpu_power(region_name)

        ## Get the average power consumption of the instance in the given region (kw_mb)
        average_memory_power: float = self._datacenter_loader.get_average_memory_power(region_name)

        ## Get the carbon free energy of the grid in the given region
        cfe: float = 0.0
        if self._consider_cfe:
            cfe = self._datacenter_loader.get_cfe(region_name)

        ## Get the power usage effectiveness of the datacenter in the given region
        pue: float = self._datacenter_loader.get_pue(region_name)

        ## Get the carbon intensity of the grid in the given region (gCO2e/kWh)
        grid_co2e: float = self._carbon_loader.get_grid_carbon_intensity(region_name)

        ## Get the number of vCPUs and Memory of the instance
        provider, _ = region_name.split(":")  # Get the provider from the region name
        vcpu: float = self._workflow_loader.get_vcpu(instance_name, provider)
        memory: float = self._workflow_loader.get_memory(instance_name, provider)

        # Calculate CO2e with those information
        ## Average power from compute
        ## Compute Watt-Hours = Average Watts * vCPU Hours
        runtime_in_hours: np.ndarray = runtime_distributions / 3600  # Seconds to hours, Element wise division
        compute_kwh: np.ndarray = average_cpu_power * vcpu * runtime_in_hours  # Element wise multiplication

        memory_kwh: np.ndarray = average_memory_power * memory * runtime_in_hours

        cloud_provider_usage_kwh: np.ndarray = compute_kwh + memory_kwh  # Element wise addition of 2 numpy arrays

        operational_emission: np.ndarray = (
            cloud_provider_usage_kwh * (1 - cfe) * pue * grid_co2e
        )  # Element wise multiplication

        # Sort the array in place
        operational_emission.sort()

        return operational_emission  # gCO2e distribution

    def calculate_transmission_carbon_distribution(
        self, from_instance_name: str, to_instance_name: str, from_region_name: str, to_region_name: str
    ) -> np.ndarray:
        # TODO (#166): This can be potentially done using what we do in this issue
        if from_instance_name == "start_hop":
            return np.array([0.0])
        # Get the data transfer size from the workflow loader (In units of GB)
        data_transfer_size_distribution: np.ndarray = np.array(
            self._workflow_loader.get_data_transfer_size_distribution(from_instance_name, to_instance_name)
        )

        data_latency_distribution: np.ndarray = self._runtime_calculator.calculate_latency_distribution(
            from_instance_name=from_instance_name,
            to_instance_name=to_instance_name,
            from_region_name=from_region_name,
            to_region_name=to_region_name,
        )

        # Get the carbon intesnity of transmission in units of gCo2eq/GB
        transmission_carbon_intensity, distance = self._carbon_loader.get_transmission_carbon_intensity(
            from_region_name, to_region_name
        )

        if CARBON_TRANSMISSION_CARBON_METHOD == "distance":
            kwh_per_gb: float = KWH_PER_GB_ESTIMATE + KWH_PER_KM_GB_ESTIMATE * distance
            transmission_carbon_intensity *= kwh_per_gb  # Here transmission_carbon_intensity is a floating point number

            # Calculate the carbon emissions
            # Carbon emissions = Data transfer size (GB) * Transmission carbon intensity (gCo2eq/GB)
            carbon_emissions_distribution = (
                data_transfer_size_distribution * transmission_carbon_intensity
            )  # Element wise multiplication

            # Sort the array in place
            carbon_emissions_distribution.sort()

            return carbon_emissions_distribution

        if CARBON_TRANSMISSION_CARBON_METHOD == "latency":
            kwh_per_gb_distribution: np.ndarray = (
                KWH_PER_GB_ESTIMATE + KWH_PER_S_GB_ESTIMATE * data_latency_distribution
            )  # Here kwh_per_gb_distribution is a distribution, result is a distribution
            transmission_carbon_intensity_distribution = (
                transmission_carbon_intensity * kwh_per_gb_distribution
            )  # Here transmission_carbon_intensity_distribution is a distribution

            # Calculate the carbon emissions
            # Carbon emissions = Data transfer size (GB) * Transmission carbon intensity (gCo2eq/GB)

            # However now we have 2 distributions, data transefer size and transmission carbon intensity
            # We must treat this differently, one approach is outer product. And we flatten the result
            carbon_emissions_distribution = np.outer(
                data_transfer_size_distribution, transmission_carbon_intensity_distribution
            ).flatten()

            # Sort the array in place
            carbon_emissions_distribution.sort()

            return carbon_emissions_distribution

        raise ValueError(f"Invalid carbon transmission method: {CARBON_TRANSMISSION_CARBON_METHOD}")
