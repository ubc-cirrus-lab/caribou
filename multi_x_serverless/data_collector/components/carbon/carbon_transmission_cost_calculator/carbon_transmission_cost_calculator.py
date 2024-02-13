import math
from abc import ABC
from typing import Any, Callable


class CarbonTransmissionCostCalculator(ABC):
    def __init__(self, get_carbon_intensity_from_coordinates: Callable) -> None:
        self._get_carbon_intensity_from_coordinates = get_carbon_intensity_from_coordinates
        self._total_distance = 0.0
        # Current resolution set to 500 km for one segment
        self._step_size = 500

    def calculate_transmission_carbon_intensity(
        self, region_from: dict[str, Any], region_to: dict[str, Any]
    ) -> tuple[float, float]:
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

        return total_carbon_intensity, self._total_distance

    def _get_distance_between_coordinates(
        self, latitude_from: float, longitude_from: float, latitude_to: float, longitude_to: float
    ) -> float:
        r = 6371.0

        lat1 = math.radians(latitude_from)
        lon1 = math.radians(longitude_from)
        lat2 = math.radians(latitude_to)
        lon2 = math.radians(longitude_to)

        # Differences in latitude and longitude
        dlat = lat2 - lat1
        dlon = lon2 - lon1

        # Haversine formula
        a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        distance = r * c

        return distance

    def _get_carbon_intensity_segments_from_coordinates(
        self, latitude_from: float, longitude_from: float, latitude_to: float, longitude_to: float
    ) -> list[tuple[float, float]]:
        self._total_distance = self._get_distance_between_coordinates(
            latitude_from, longitude_from, latitude_to, longitude_to
        )

        current_location = (latitude_from, longitude_from)
        current_distance: float = self._step_size
        current_carbon_intensity = self._get_carbon_intensity_from_coordinates(latitude_from, longitude_from)

        segment_distance: float = self._step_size

        segments: list[tuple[float, float]] = []

        while current_distance < self._total_distance:
            # Calculate the step size in degrees for latitude and longitude
            step_size_deg = (
                segment_distance / 111.32,
                segment_distance / (111.32 * math.cos(math.radians(current_location[0]))),
            )

            next_location = (
                current_location[0] + step_size_deg[0],
                current_location[1] + step_size_deg[1],
            )
            next_carbon_intensity = self._get_carbon_intensity_from_coordinates(next_location[0], next_location[1])

            if next_carbon_intensity != current_carbon_intensity:
                # Add the segment to the list when carbon intensity changes
                segments.append((segment_distance, current_carbon_intensity))
                current_carbon_intensity = next_carbon_intensity
                current_location = next_location
                segment_distance = self._step_size
            else:
                segment_distance += self._step_size

            current_distance += self._step_size

        # Calculate the remaining segment for the final location
        segment_distance = segment_distance - (current_distance - self._total_distance)
        step_size_deg = (
            segment_distance / 111.32,
            segment_distance / (111.32 * math.cos(math.radians(current_location[0]))),
        )
        last_next_location = (
            current_location[0] + step_size_deg[0],
            current_location[1] + step_size_deg[1],
        )
        current_carbon_intensity = self._get_carbon_intensity_from_coordinates(
            last_next_location[0], last_next_location[1]
        )
        segments.append((segment_distance, current_carbon_intensity))

        return segments
