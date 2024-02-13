from multi_x_serverless.common.constants import (
    CARBON_TRANSMISSION_CARBON_METHOD,
    KWH_PER_GB_ESTIMATE,
    KWH_PER_KM_GB_ESTIMATE,
    KWH_PER_S_GB_ESTIMATE,
)
from multi_x_serverless.routing.solver.input.components.calculator import InputCalculator
from multi_x_serverless.routing.solver.input.components.calculators.runtime_calculator import RuntimeCalculator
from multi_x_serverless.routing.solver.input.components.loaders.carbon_loader import CarbonLoader
from multi_x_serverless.routing.solver.input.components.loaders.datacenter_loader import DatacenterLoader
from multi_x_serverless.routing.solver.input.components.loaders.workflow_loader import WorkflowLoader


class CarbonCalculator(InputCalculator):
    def __init__(
        self,
        carbon_loader: CarbonLoader,
        datacenter_loader: DatacenterLoader,
        workflow_loader: WorkflowLoader,
        runtime_calculator: RuntimeCalculator,
    ) -> None:
        super().__init__()
        self._carbon_loader: CarbonLoader = carbon_loader
        self._datacenter_loader: DatacenterLoader = datacenter_loader
        self._workflow_loader: WorkflowLoader = workflow_loader
        self._runtime_calculator: RuntimeCalculator = runtime_calculator

    def calculate_execution_carbon(
        self, instance_name: str, region_name: str, consider_probabilistic_invocations: bool = False
    ) -> float:
        if consider_probabilistic_invocations:
            # TODO (#76): Implement probabilistic invocations
            raise NotImplementedError("Probabilistic invocations are not supported yet")

        return self._calculate_raw_execution_carbon(instance_name, region_name, False)

    def calculate_transmission_carbon(
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

        return self._calculate_raw_transmission_carbon(
            from_instance_name, to_instance_name, from_region_name, to_region_name
        )

    def _calculate_raw_execution_carbon(
        self, instance_name: str, region_name: str, use_tail_latency: bool = False
    ) -> float:
        # Retrieve or format input information

        ## Get the runtime of the instance in the given region (s)
        runtime = self._runtime_calculator.calculate_raw_runtime(instance_name, region_name, use_tail_latency)

        ## datacenter loader data
        average_cpu_power = self._datacenter_loader.get_average_cpu_power(
            region_name
        )  # Get the average power consumption of the instance in the given region (kw_compute)
        average_memory_power = self._datacenter_loader.get_average_memory_power(
            region_name
        )  # Get the average power consumption of the instance in the given region (kw_mb)
        cfe = self._datacenter_loader.get_cfe(region_name)  # Get the carbon free energy of the grid in the given region
        pue = self._datacenter_loader.get_pue(
            region_name
        )  # Get the power usage effectiveness of the datacenter in the given region

        ## Get the carbon intensity of the grid in the given region (gCO2e/kWh)
        grid_co2e = self._carbon_loader.get_grid_carbon_intensity(region_name)

        ## Get the number of vCPUs and Memory of the instance
        provider, _ = region_name.split(":")  # Get the provider from the region name
        vcpu = self._workflow_loader.get_vcpu(instance_name, provider)
        memory = self._workflow_loader.get_memory(instance_name, provider)

        # Calculate CO2e with those information
        ## Average power from compute
        ## Compute Watt-Hours = Average Watts * vCPU Hours
        runtime_in_hours = runtime / 3600  # Seconds to hours
        compute_kwh = average_cpu_power * vcpu * runtime_in_hours

        memory_kwh = average_memory_power * memory * runtime_in_hours

        cloud_provider_usage_kwh = compute_kwh + memory_kwh

        operational_emission = cloud_provider_usage_kwh * (1 - cfe) * pue * grid_co2e

        return operational_emission  # gCO2e

    def _calculate_raw_transmission_carbon(
        self, from_instance_name: str, to_instance_name: str, from_region_name: str, to_region_name: str
    ) -> float:
        # Get the data transfer size from the workflow loader (In units of GB)
        data_transfer_size = self._workflow_loader.get_data_transfer_size(from_instance_name, to_instance_name)

        data_latency = self._workflow_loader.get_latency(
            from_instance_name, to_instance_name, from_region_name, to_region_name
        )

        # Get the carbon intesnity of transmission in units of gCo2eq/GB
        transmission_carbon_intensity, distance = self._carbon_loader.get_transmission_carbon_intensity(
            from_region_name, to_region_name
        )

        if CARBON_TRANSMISSION_CARBON_METHOD == "distance":
            kwh_per_gb = KWH_PER_GB_ESTIMATE + KWH_PER_KM_GB_ESTIMATE * distance
            transmission_carbon_intensity *= kwh_per_gb
        elif CARBON_TRANSMISSION_CARBON_METHOD == "latency":
            kwh_per_gb = KWH_PER_GB_ESTIMATE + KWH_PER_S_GB_ESTIMATE * data_latency
            transmission_carbon_intensity *= kwh_per_gb

        # Calculate the carbon emissions
        # Carbon emissions = Data transfer size (GB) * Transmission carbon intensity (gCo2eq/GB)
        return data_transfer_size * transmission_carbon_intensity
