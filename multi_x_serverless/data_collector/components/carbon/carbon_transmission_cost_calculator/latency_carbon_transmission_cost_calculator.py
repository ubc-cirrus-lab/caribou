from typing import Any, Callable, Optional

from multi_x_serverless.common.provider import Provider
from multi_x_serverless.data_collector.components.carbon.carbon_transmission_cost_calculator.carbon_transmission_cost_calculator import (  # pylint: disable=line-too-long
    CarbonTransmissionCostCalculator,
)
from multi_x_serverless.data_collector.utils.latency_retriever.aws_latency_retriever import AWSLatencyRetriever
from multi_x_serverless.data_collector.utils.latency_retriever.integration_test_latency_retriever import (
    IntegrationTestLatencyRetriever,
)


class LatencyCarbonTransmissionCostCalculator(CarbonTransmissionCostCalculator):
    def __init__(self, get_carbon_intensity_from_coordinates: Callable, config: Optional[dict] = None) -> None:
        super().__init__(get_carbon_intensity_from_coordinates, config)
        if config is not None and "kwh_per_km_gb_estimate" in config:
            self._kwh_per_ms_gb_estimate = config["kwh_per_km_gb_estimate"]
        else:
            self._kwh_per_ms_gb_estimate = 0.0000166667
        self._aws_latency_retriever = AWSLatencyRetriever()
        self._integration_test_latency_retriever = IntegrationTestLatencyRetriever()

    def calculate_transmission_carbon_intensity(self, region_from: dict[str, Any], region_to: dict[str, Any]) -> float:
        latitude_from = region_from["latitude"]
        longitude_from = region_from["longitude"]
        latitude_to = region_to["latitude"]
        longitude_to = region_to["longitude"]

        carbon_intensity_segments = self._get_carbon_intensity_segments_from_coordinates(
            latitude_from, longitude_from, latitude_to, longitude_to
        )

        if self._total_distance == 0.0:
            raise ValueError("Total distance is 0.0, cannot calculate carbon intensity")

        total_carbon_intensity: float = 0.0
        for segment in carbon_intensity_segments:
            segment_relative_distance_weight = segment[0] / self._total_distance
            total_carbon_intensity += segment_relative_distance_weight * segment[1]

        return total_carbon_intensity
