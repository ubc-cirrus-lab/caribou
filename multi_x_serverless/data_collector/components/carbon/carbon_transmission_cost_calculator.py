from abc import ABC, abstractmethod
from typing import Any
import math


class CarbonTransmissionCostCalculator(ABC):
    def __init__(self, config: dict, get_carbon_intensity_from_coordinates: callable) -> None:
        self._config = config
        self._get_carbon_intensity_from_coordinates = get_carbon_intensity_from_coordinates
        if "kwh_per_gb_estimate" in config:
            self._kwh_per_gb_estimate = config["kwh_per_gb_estimate"]
        else:
            self._kwh_per_gb_estimate = 0.1

    @abstractmethod
    def calculate_transmission_carbon_intensity(self, region_from: dict[str, Any], region_to: dict[str, Any]) -> float:
        raise NotImplementedError


    def _get_distance_between_coordinates(
        self, latitude_from: float, longitude_from: float, latitude_to: float, longitude_to: float
    ) -> float:
        R = 6371.0

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
        distance = R * c

        return distance


    def _get_carbon_intensity_segments_from_coordinates(
        self, latitude_from: float, longitude_from: float, latitude_to: float, longitude_to: float
    ) -> float:
        self._total_distance = self._get_distance_between_coordinates(
            latitude_from, longitude_from, latitude_to, longitude_to
        )

        current_location = (latitude_from, longitude_from)
        current_distance: float = 0.0
        current_carbon_intensity = self._get_carbon_intensity_from_coordinates(latitude_from, longitude_from)
        segment_distance: float = 0.0

        segments: list[tuple[float, float]] = []

        # Current resolution set to 250 km for one segment
        step_size = 250

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
                segment_distance = 0
            else:
                segment_distance += step_size

            current_distance += step_size

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
