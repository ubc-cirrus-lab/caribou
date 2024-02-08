from typing import Any, Callable, Optional

from multi_x_serverless.data_collector.components.carbon.carbon_transmission_cost_calculator.carbon_transmission_cost_calculator import (  # pylint: disable=line-too-long
    CarbonTransmissionCostCalculator,
)


class DistanceCarbonTransmissionCostCalculator(CarbonTransmissionCostCalculator):
    def __init__(self, get_carbon_intensity_from_coordinates: Callable, config: Optional[dict] = None) -> None:
        super().__init__(get_carbon_intensity_from_coordinates, config)
        if config is not None and "kwh_per_km_gb_estimate" in config:
            self._kwh_per_km_gb_estimate = config["kwh_per_km_gb_estimate"]
        else:
            self._kwh_per_km_gb_estimate = 0.005

    def calculate_transmission_carbon_intensity(self, region_from: dict[str, Any], region_to: dict[str, Any]) -> float:
        latitude_from = region_from["latitude"]
        longitude_from = region_from["longitude"]
        latitude_to = region_to["latitude"]
        longitude_to = region_to["longitude"]

        carbon_intensity_segments = self._get_carbon_intensity_segments_from_coordinates(
            latitude_from, longitude_from, latitude_to, longitude_to
        )

        total_carbon_intensity = 0.0
        for segment in carbon_intensity_segments:
            total_carbon_intensity += self._calculate_carbon_intensity_segment(segment[0], segment[1])

        return total_carbon_intensity

    def _calculate_carbon_intensity_segment(self, segment_distance: float, gco2e_per_kwh: float) -> float:
        kwh_per_gb = self._kwh_per_gb_estimate + self._kwh_per_km_gb_estimate * segment_distance
        gco2e_per_gb = gco2e_per_kwh * kwh_per_gb
        return gco2e_per_gb
