import datetime
import math
import os
import time
from typing import Any

import requests

from multi_x_serverless.data_collector.components.data_retriever import DataRetriever
from multi_x_serverless.deployment.common.remote_client.remote_client import RemoteClient


class CarbonRetriever(DataRetriever):
    def __init__(self, client: RemoteClient) -> None:
        super().__init__(client)
        self._electricity_maps_auth_token = os.environ["ELECTRICITY_MAPS_AUTH_TOKEN"]
        self._request_backoff = 0.1
        self._last_request = datetime.datetime.now()
        self._global_average_worst_case_carbon_intensity = 475.0
        self._kwh_per_gb_estimate = 1.0

    def retrieve_at_region_carbon_data(self) -> dict[str, dict[str, Any]]:
        result_dict: dict[str, dict[str, Any]] = {}
        for region_key, available_region in self._available_regions.items():
            latitude = available_region["latitude"]
            longitude = available_region["longitude"]
            carbon_intensity = self._get_carbon_intensity_from_coordinates(latitude, longitude)

            result_dict[region_key] = {
                "carbon_intensity": carbon_intensity,
                "unit": "gCO2eq/kWh",
            }
        return result_dict

    def retrieve_from_to_region_carbon_data(self) -> dict[str, dict[str, Any]]:
        from_region_data: dict[str, dict[str, Any]] = {}
        for region_key_from, available_region_from in self._available_regions.items():
            to_region_data: dict[str, Any] = {}
            for region_key_to, available_region_to in self._available_regions.items():
                if region_key_from == region_key_to:
                    continue

                latitude_from = available_region_from["latitude"]
                longitude_from = available_region_from["longitude"]
                latitude_to = available_region_to["latitude"]
                longitude_to = available_region_to["longitude"]

                carbon_intensity_gco2e_per_gb = self._get_transmission_carbon_intensity_from_coordinates(
                    latitude_from, longitude_from, latitude_to, longitude_to
                )

                to_region_data[region_key_to] = {
                    "carbon_intensity": carbon_intensity_gco2e_per_gb,
                    "unit": "gCO2eq/GB",
                }
            from_region_data[region_key_from] = to_region_data
        return from_region_data

    def _get_transmission_carbon_intensity_from_coordinates(
        self, latitude_from: float, longitude_from: float, latitude_to: float, longitude_to: float
    ) -> float:
        total_distance = self._get_distance_between_coordinates(
            latitude_from, longitude_from, latitude_to, longitude_to
        )

        total_carbon_intensity: float = 0.0
        current_location = (latitude_from, longitude_from)
        current_distance: float = 0.0
        current_carbon_intensity = self._get_carbon_intensity_from_coordinates(latitude_from, longitude_from)
        segment_distance: float = 0.0

        # Current resolution set to 250 km for one segment
        step_size = 250

        while current_distance < total_distance:
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
                total_carbon_intensity += self._calculate_carbon_intensity_segment(
                    segment_distance, current_carbon_intensity
                )
                current_carbon_intensity = next_carbon_intensity
                current_location = next_location
                segment_distance = 0
            else:
                segment_distance += step_size

            current_distance += step_size

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
        total_carbon_intensity += self._calculate_carbon_intensity_segment(segment_distance, current_carbon_intensity)

        return total_carbon_intensity

    def _calculate_carbon_intensity_segment(self, segment_distance: float, segment_carbon_intensity: float) -> float:
        return segment_distance * segment_carbon_intensity * self._kwh_per_gb_estimate

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

    def _get_carbon_intensity_from_coordinates(self, latitude: float, longitude: float) -> float:
        electricitymaps = "https://api-access.electricitymaps.com/free-tier/carbon-intensity/latest?"

        if (datetime.datetime.now() - self._last_request).total_seconds() < self._request_backoff:
            time.sleep(self._request_backoff)

        response = requests.get(
            electricitymaps + "lat=" + str(latitude) + "&lon=" + str(longitude),
            headers={"auth-token": self._electricity_maps_auth_token},
            timeout=5,
        )

        self._last_request = datetime.datetime.now()

        if response.status_code == 200:
            json_data = response.json()

            if "carbonIntensity" in json_data:
                return json_data["carbonIntensity"]
            else:
                raise ValueError("Could not find carbon intensity in response")

        if response.status_code == 404 and "No recent data for zone" in response.text:
            return self._global_average_worst_case_carbon_intensity

        raise ValueError(f"Could not retrieve carbon intensity from Electricity Maps API: {response.text}")
