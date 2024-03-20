import math
import os
import time
from datetime import datetime, timedelta
from functools import partial
from typing import Any, Callable

import requests

from multi_x_serverless.common.constants import GLOBAL_TIME_ZONE
from multi_x_serverless.common.models.remote_client.remote_client import RemoteClient
from multi_x_serverless.common.utils import str_to_bool
from multi_x_serverless.data_collector.components.data_retriever import DataRetriever


class CarbonRetriever(DataRetriever):  # pylint: disable=too-many-instance-attributes
    def __init__(self, client: RemoteClient) -> None:
        super().__init__(client)
        self._integration_test_on = str_to_bool(os.environ.get("INTEGRATIONTEST_ON", "False"))

        self._electricity_maps_auth_token = os.environ.get("ELECTRICITY_MAPS_AUTH_TOKEN")

        if self._electricity_maps_auth_token is None and not self._integration_test_on:
            raise ValueError("ELECTRICITY_MAPS_AUTH_TOKEN environment variable not set")

        self._request_backoff = 0.1
        self._last_request = datetime.now(GLOBAL_TIME_ZONE)
        self._global_average_worst_case_carbon_intensity = 475.0

        # Must be in UTC And enforced in the format "YYYY-MM-DDTHH:MM:SSZ"
        self._start_timestamp = (
            (datetime.now(GLOBAL_TIME_ZONE) - timedelta(days=6, hours=23))
            .replace(minute=0, second=0, microsecond=0)
            .isoformat()
            .replace("+00:00", "Z")
        )
        self._end_timestamp = (
            datetime.now(GLOBAL_TIME_ZONE).replace(minute=0, second=0, microsecond=0).isoformat().replace("+00:00", "Z")
        )

        self._carbon_intensity_history_cache: dict[tuple[float, float], dict[str, Any]] = {}

        self._carbon_intensity_cache: dict[tuple[float, float], float] = {}

    def retrieve_carbon_region_data(self) -> dict[str, dict[str, Any]]:
        result_dict: dict[str, dict[str, Any]] = {}
        for region_key, available_region in self._available_regions.items():
            # We have 2 methods to retrieve the carbon intensity
            # One is overall average carbon intensity
            # Another one is hourly average carbon intensity

            ## Overall average carbon intensity
            overall_average_data = self._get_execution_carbon_intensity(
                available_region, self._get_overall_average_carbon_intensity
            )

            ## Hourly average carbon intensity
            # For this we need a 24 hour loop
            hourly_average_data: dict[str, dict[str, Any]] = {}
            for hour in range(24):
                hourly_average_data[str(hour)] = self._get_execution_carbon_intensity(
                    available_region, partial(self._get_hour_average_carbon_intensity, hour=hour)
                )

            averages = {"overall": overall_average_data, **hourly_average_data}

            # Store the result
            result_dict[region_key] = {
                "averages": averages,
                "units": "gCO2eq/kWh",
                "transmission_distances": self._get_distance_between_all_regions(available_region),
                "transmission_distances_unit": "km",
            }

        return result_dict

    def _get_execution_carbon_intensity(
        self, available_region: dict[str, Any], get_carbon_intensity_from_coordinates: Callable
    ) -> dict[str, Any]:
        latitude = available_region["latitude"]
        longitude = available_region["longitude"]

        carbon_intensity = get_carbon_intensity_from_coordinates(latitude, longitude)

        return {
            "carbon_intensity": carbon_intensity,
        }

    def _get_distance_between_all_regions(self, from_region: dict[str, float]) -> dict[str, float]:
        distance_dict: dict[str, float] = {}
        for region_key_to, available_region_to in self._available_regions.items():
            distance = self._get_distance_between_coordinates(
                from_region["latitude"],
                from_region["longitude"],
                available_region_to["latitude"],
                available_region_to["longitude"],
            )

            distance_dict[region_key_to] = distance

        return distance_dict

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

    def _get_hour_average_carbon_intensity(self, latitude: float, longitude: float, hour: int) -> float:
        if self._integration_test_on:
            return latitude + longitude

        return self._get_carbon_intensity_information(latitude, longitude)["hourly_average"].get(
            hour, self._global_average_worst_case_carbon_intensity
        )

    def _get_overall_average_carbon_intensity(self, latitude: float, longitude: float) -> float:
        if self._integration_test_on:
            return latitude + longitude

        return self._get_carbon_intensity_information(latitude, longitude)["overall_average"]

    def _get_carbon_intensity_information(self, latitude: float, longitude: float) -> dict[str, Any]:
        # Check cache if the carbon intensity is already there
        if (latitude, longitude) in self._carbon_intensity_history_cache:
            return self._carbon_intensity_history_cache[(latitude, longitude)]

        # If not, retrieve then process the raw carbon intensity history
        raw_carbon_intensity_history = self._get_raw_carbon_intensity_history_range(
            latitude, longitude, self._start_timestamp, self._end_timestamp
        )
        processed_carbon_intensity = self._process_raw_carbon_intensity_history(raw_carbon_intensity_history)
        self._carbon_intensity_history_cache[(latitude, longitude)] = processed_carbon_intensity

        return processed_carbon_intensity

    def _process_raw_carbon_intensity_history(
        self, raw_carbon_intensity_history: list[dict[str, str]]
    ) -> dict[str, Any]:
        if (len(raw_carbon_intensity_history)) == 0:
            return {
                "overall_average": self._global_average_worst_case_carbon_intensity,
                "hourly_average": {hour: self._global_average_worst_case_carbon_intensity for hour in range(24)},
            }
        # We must process the raw information to aquire the necessary plans
        # One of the plan is to get the hourly carbon intenity
        # Another one is just overall average of the carbon intensity

        # First convert to ordered history
        total_carbon_intensity = 0.0
        ordered_history: dict[int, list[float]] = {}
        for time_slot in raw_carbon_intensity_history:
            # Parse the datetime string into a datetime object
            datetime_obj = datetime.fromisoformat(
                time_slot["datetime"].replace("Z", "")
            )  # For fromisoformat doesn't handle 'Z'.

            # Retrieve the hour section
            hour = datetime_obj.hour

            # Create entry on carbon history if needed
            if hour not in ordered_history:
                ordered_history[hour] = []

            # For carbon information
            carbon_intensity = float(time_slot["carbonIntensity"])
            total_carbon_intensity += carbon_intensity

            ordered_history[hour].append(carbon_intensity)

        # For for each hour we want to get the average carbon intensity
        hourly_average_carbon_intensity: dict[int, float] = {}
        for hour, carbon_intensity_list in ordered_history.items():
            hourly_average_carbon_intensity[hour] = sum(carbon_intensity_list) / len(carbon_intensity_list)

        average_carbon_intensity = total_carbon_intensity / len(raw_carbon_intensity_history)

        return {
            "overall_average": average_carbon_intensity,
            "hourly_average": hourly_average_carbon_intensity,
        }

    def _get_raw_carbon_intensity_history_range(
        self, latitude: float, longitude: float, start_timestamp: str, end_timestamp: str
    ) -> list[dict[str, str]]:
        electricitymaps = "https://api-access.electricitymaps.com/free-tier/carbon-intensity/past-range?"

        if (datetime.now(GLOBAL_TIME_ZONE) - self._last_request).total_seconds() < self._request_backoff:
            time.sleep(self._request_backoff)

        if self._electricity_maps_auth_token is None:
            raise ValueError("ELECTRICITY_MAPS_AUTH_TOKEN environment variable not set")

        response = requests.get(
            electricitymaps
            + "lat="
            + str(latitude)
            + "&lon="
            + str(longitude)
            + "&start="
            + start_timestamp
            + "&end="
            + end_timestamp,
            headers={"auth-token": self._electricity_maps_auth_token},
            timeout=10,
        )

        self._last_request = datetime.now(GLOBAL_TIME_ZONE)

        result: list[dict[str, str]] = []

        if response.status_code == 200:
            json_data = response.json()

            if "data" in json_data:
                result = json_data["data"]

        return result

    def _get_carbon_intensity_from_coordinates(self, latitude: float, longitude: float) -> float:
        # This is a legacy function, for now it is not used, it is here as it may be used in the future
        if self._integration_test_on:
            return latitude + longitude

        if (latitude, longitude) in self._carbon_intensity_cache:
            return self._carbon_intensity_cache[(latitude, longitude)]
        electricitymaps = "https://api-access.electricitymaps.com/free-tier/carbon-intensity/latest?"

        if (datetime.now(GLOBAL_TIME_ZONE) - self._last_request).total_seconds() < self._request_backoff:
            time.sleep(self._request_backoff)

        if self._electricity_maps_auth_token is None:
            raise ValueError("ELECTRICITY_MAPS_AUTH_TOKEN environment variable not set")

        response = requests.get(
            electricitymaps + "lat=" + str(latitude) + "&lon=" + str(longitude),
            headers={"auth-token": self._electricity_maps_auth_token},
            timeout=10,
        )

        self._last_request = datetime.now(GLOBAL_TIME_ZONE)

        result = self._global_average_worst_case_carbon_intensity

        if response.status_code == 200:
            json_data = response.json()

            if "carbonIntensity" in json_data:
                result = json_data["carbonIntensity"]

        self._carbon_intensity_cache[(latitude, longitude)] = result
        return result
