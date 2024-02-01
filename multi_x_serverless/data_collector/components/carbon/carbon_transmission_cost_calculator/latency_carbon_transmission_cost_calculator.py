from typing import Any, Callable

from multi_x_serverless.common.provider import Provider
from multi_x_serverless.data_collector.components.carbon.carbon_transmission_cost_calculator.carbon_transmission_cost_calculator import (
    CarbonTransmissionCostCalculator,
)
from multi_x_serverless.data_collector.utils.aws_latency_retriever import AWSLatencyRetriever


class LatencyCarbonTransmissionCostCalculator(CarbonTransmissionCostCalculator):
    def __init__(self, config: dict, get_carbon_intensity_from_coordinates: Callable) -> None:
        super().__init__(config, get_carbon_intensity_from_coordinates)
        if "_kwh_per_km_gb_estimate" in config:
            self._kwh_per_ms_gb_estimate = config["_kwh_per_km_gb_estimate"]
        else:
            self._kwh_per_ms_gb_estimate = 0.0000166667
        self._aws_latency_retriever = AWSLatencyRetriever()

    def calculate_transmission_carbon_intensity(self, region_from: dict[str, Any], region_to: dict[str, Any]) -> float:
        total_latency = self._get_total_latency(region_from, region_to)

        latitude_from = region_from["latitude"]
        longitude_from = region_from["longitude"]
        latitude_to = region_to["latitude"]
        longitude_to = region_to["longitude"]

        carbon_intensity_segments = self._get_carbon_intensity_segments_from_coordinates(
            latitude_from, longitude_from, latitude_to, longitude_to
        )

        total_carbon_intensity: float = 0.0
        for segment in carbon_intensity_segments:
            segment_relative_distance_weight = segment[0] / self._total_distance
            segment_relative_latency = segment_relative_distance_weight * total_latency
            total_carbon_intensity += self._calculate_carbon_intensity_segment(segment_relative_latency, segment[1])

        return total_carbon_intensity

    def _calculate_carbon_intensity_segment(self, segment_latency_ms: float, gCO2e_per_kWh: float) -> float:
        kWh_per_gb = self._kwh_per_gb_estimate + self._kwh_per_ms_gb_estimate * segment_latency_ms
        gCO2e_per_gb = gCO2e_per_kWh * kWh_per_gb
        return gCO2e_per_gb

    def _get_total_latency(self, region_from: dict[str, Any], region_to: dict[str, Any]) -> float:
        if region_from["provider"] == region_to["provider"] and region_from["provider"] == Provider.AWS.value:
            return self._aws_latency_retriever.get_latency(region_from, region_to)

        return 0.0  # Default value, maybe a better default or an error message will be desired
