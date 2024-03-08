import math
from typing import Any, Callable


class CarbonTransmissionCostCalculator:
    def __init__(self, get_carbon_intensity_from_coordinates: Callable) -> None:
        self._get_carbon_intensity_from_coordinates = get_carbon_intensity_from_coordinates
        # Current resolution set to 500 km for one segment
        self._step_size = 500

    def calculate_transmission_carbon_intensity(
        self, region_from: dict[str, Any], region_to: dict[str, Any]
    ) -> tuple[float, float]:
        latitude_from = region_from["latitude"]
        longitude_from = region_from["longitude"]
        latitude_to = region_to["latitude"]
        longitude_to = region_to["longitude"]

        total_distance = self._get_distance_between_coordinates(
            latitude_from, longitude_from, latitude_to, longitude_to
        )

        carbon_intensity_segments = self._get_carbon_intensity_segments_from_coordinates(
            latitude_from, longitude_from, latitude_to, longitude_to, total_distance
        )

        if total_distance == 0.0:
            return 0, total_distance

        total_carbon_intensity: float = 0.0
        for segment in carbon_intensity_segments:
            segment_relative_distance_weight = segment[0] / total_distance
            total_carbon_intensity += segment_relative_distance_weight * segment[1]

        return total_carbon_intensity, total_distance

    def _get_carbon_intensity_segments_from_coordinates(
        self,
        latitude_from: float,
        longitude_from: float,
        latitude_to: float,
        longitude_to: float,
        total_distance: float,
    ) -> list[tuple[float, float]]:
        current_location = (latitude_from, longitude_from)
        current_distance: float = self._step_size
        current_carbon_intensity = self._get_carbon_intensity_from_coordinates(latitude_from, longitude_from)

        segment_distance: float = self._step_size

        segments: list[tuple[float, float]] = []

        y = math.sin(longitude_to - longitude_from) * math.cos(latitude_to)
        x = math.cos(latitude_from) * math.sin(latitude_to) - math.sin(latitude_from) * math.cos(
            latitude_to
        ) * math.cos(longitude_to - longitude_from)
        bearing = math.atan2(y, x)

        while current_distance < total_distance:
            # Calculate the new latitude and longitude based on the bearing and step size
            lat2 = math.asin(
                math.sin(latitude_from) * math.cos(segment_distance)
                + math.cos(latitude_from) * math.sin(segment_distance) * math.cos(bearing)
            )
            lon2 = longitude_from + math.atan2(
                math.sin(bearing) * math.sin(segment_distance) * math.cos(latitude_from),
                math.cos(segment_distance) - math.sin(latitude_from) * math.sin(lat2),
            )

            next_location = (lat2, lon2)
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
        segment_distance = segment_distance - (current_distance - total_distance)
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
