from typing import Optional
import os

from multi_x_serverless.common.constants import DFI, DFM  # KWH_PER_GB_ESTIMATE,
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
        self._transmission_conversion_ratio_cache: dict[str, float] = {}

        # Carbon setting - hourly or average policy
        self._hourly_carbon_setting: Optional[str] = None  # None indicates the default setting -> Average everything

        self.small_or_large = "small"
        self.energy_factor = 0.005
        self.home_base_case = True

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

        utilization = self.get_cpu_utilization(instance_name)
        average_cpu_power = (0.74 + utilization * (3.5 - 0.74)) / 1000
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

        # Covert memory in MB to GB
        memory = memory / 1024

        compute_factor = average_cpu_power * vcpu / 3600
        memory_factor = average_memory_power * memory / 3600
        power_factor = (1 - cfe) * pue * grid_co2e

        # Add the conversion ratio to the cache
        self._execution_conversion_ratio_cache[cache_key] = (compute_factor, memory_factor, power_factor)
        return self._execution_conversion_ratio_cache[cache_key]

    def calculate_transmission_carbon(
        self, from_region_name: str, to_region_name: str, data_transfer_size: float
    ) -> float:
        if self.get_home_base_case() and from_region_name == to_region_name:
            return 0
        # distance_factor_distance = self._get_transmission_conversion_ratio(from_region_name, to_region_name)
        from_region_carbon_intensity: float = self._carbon_loader.get_grid_carbon_intensity(
            from_region_name, self._hourly_carbon_setting
        )
        to_region_carbon_intensity: float = self._carbon_loader.get_grid_carbon_intensity(
            to_region_name, self._hourly_carbon_setting
        )

        transmission_carbon_intensity = (from_region_carbon_intensity + to_region_carbon_intensity) / 2
        return data_transfer_size * self.get_energy_factor() * transmission_carbon_intensity

    def get_home_base_case(self) -> bool:
        return self.home_base_case

    def get_energy_factor(self) -> float:
        return self.energy_factor

    def get_small_or_large(self) -> str:
        return self.small_or_large

    def get_cpu_utilization(self, instance) -> float:
        data = {
            "dna_visualization-0_0_1-Visualize:entry_point:0": 0.4618928777116529,
            "image_processing-0_0_1-Flip:entry_point:0": 0.41624727417345264,
            "image_processing-0_0_1-Rotate:image_processing-0_0_1-Flip_0_0:1": 0.46127630837161965,
            "image_processing-0_0_1-Filter:image_processing-0_0_1-Rotate_1_0:2": 0.4182879377431907,
            "image_processing-0_0_1-Greyscale:image_processing-0_0_1-Filter_2_0:3": 0.4522796352583587,
            "image_processing-0_0_1-Resize:image_processing-0_0_1-Greyscale_3_0:4": 0.4045534859112935,
            "map_reduce-0_0_1-Input-Processor:entry_point:0": 0.741661790255507,
            "map_reduce-0_0_1-Mapper-Function:map_reduce-0_0_1-Input-Processor_0_0:1": 0.5478792050245204,
            "map_reduce-0_0_1-Mapper-Function:map_reduce-0_0_1-Input-Processor_0_1:2": 0.5478792050245204,
            "map_reduce-0_0_1-Mapper-Function:map_reduce-0_0_1-Input-Processor_0_2:3": 0.5478792050245204,
            "map_reduce-0_0_1-Mapper-Function:map_reduce-0_0_1-Input-Processor_0_3:4": 0.5478792050245204,
            "map_reduce-0_0_1-Mapper-Function:map_reduce-0_0_1-Input-Processor_0_4:5": 0.5478792050245204,
            "map_reduce-0_0_1-Mapper-Function:map_reduce-0_0_1-Input-Processor_0_5:6": 0.5478792050245204,
            "map_reduce-0_0_1-Shuffler-Function:sync:": 0.8709256844850065,
            "map_reduce-0_0_1-Reducer-Function:map_reduce-0_0_1-Shuffler-Function__0:13": 0.7165419783873649,
            "map_reduce-0_0_1-Reducer-Function:map_reduce-0_0_1-Shuffler-Function__1:14": 0.7165419783873649,
            "map_reduce-0_0_1-Output-Processor:sync:": 0.6889036122129203,
            "text_2_speech_censoring-0_0_1-GetInput:entry_point:0": 0.5827303870793014,
            "text_2_speech_censoring-0_0_1-Text2Speech:text_2_speech_censoring-0_0_1-GetInput_0_0:1": 0.1309374174243177,
            "text_2_speech_censoring-0_0_1-Profanity:text_2_speech_censoring-0_0_1-GetInput_0_1:2": 0.2609512655425868,
            "text_2_speech_censoring-0_0_1-Conversion:text_2_speech_censoring-0_0_1-Text2Speech_1_0:3": 0.13728837120707016,
            "text_2_speech_censoring-0_0_1-Censor:sync:": 0.4250452007189924,
            "text_2_speech_censoring-0_0_1-Compression:text_2_speech_censoring-0_0_1-Conversion_3_0:5": 0.17143307371131536,
            "video_analytics-0_0_1-GetInput:entry_point:0": 0.10284755744053767,
            "video_analytics-0_0_1-Streaming:video_analytics-0_0_1-GetInput_0_0:1": 0.631461477635479,
            "video_analytics-0_0_1-Streaming:video_analytics-0_0_1-GetInput_0_1:2": 0.631461477635479,
            "video_analytics-0_0_1-Streaming:video_analytics-0_0_1-GetInput_0_2:3": 0.631461477635479,
            "video_analytics-0_0_1-Streaming:video_analytics-0_0_1-GetInput_0_3:4": 0.631461477635479,
            "video_analytics-0_0_1-Decode:video_analytics-0_0_1-Streaming_1_0:5": 0.17834203510952856,
            "video_analytics-0_0_1-Decode:video_analytics-0_0_1-Streaming_2_0:6": 0.17834203510952856,
            "video_analytics-0_0_1-Decode:video_analytics-0_0_1-Streaming_3_0:7": 0.17834203510952856,
            "video_analytics-0_0_1-Decode:video_analytics-0_0_1-Streaming_4_0:8": 0.17834203510952856,
            "video_analytics-0_0_1-Recognition:video_analytics-0_0_1-Decode_5_0:9": 0.6306577177541596,
            "video_analytics-0_0_1-Recognition:video_analytics-0_0_1-Decode_6_0:10": 0.6306577177541596,
            "video_analytics-0_0_1-Recognition:video_analytics-0_0_1-Decode_7_0:11": 0.6306577177541596,
            "video_analytics-0_0_1-Recognition:video_analytics-0_0_1-Decode_8_0:12": 0.6306577177541596,
        }

        return data[instance]

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
