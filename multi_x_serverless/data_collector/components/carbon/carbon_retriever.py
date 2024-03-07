import datetime
import os
import time
from typing import Any

import requests

from multi_x_serverless.common.constants import GLOBAL_TIME_ZONE
from multi_x_serverless.common.models.remote_client.remote_client import RemoteClient
from multi_x_serverless.common.utils import str_to_bool
from multi_x_serverless.data_collector.components.carbon.carbon_transmission_cost_calculator.carbon_transmission_cost_calculator import (  # pylint: disable=line-too-long
    CarbonTransmissionCostCalculator,
)
from multi_x_serverless.data_collector.components.data_retriever import DataRetriever


class CarbonRetriever(DataRetriever):  # pylint: disable=too-many-instance-attributes
    def __init__(self, client: RemoteClient) -> None:
        super().__init__(client)
        self._integration_test_on = str_to_bool(os.environ.get("INTEGRATIONTEST_ON", "False"))

        self._electricity_maps_auth_token = os.environ.get("ELECTRICITY_MAPS_AUTH_TOKEN")

        if self._electricity_maps_auth_token is None and not self._integration_test_on:
            raise ValueError("ELECTRICITY_MAPS_AUTH_TOKEN environment variable not set")

        self._request_backoff = 0.1
        self._last_request = datetime.datetime.now(GLOBAL_TIME_ZONE)
        self._global_average_worst_case_carbon_intensity = 475.0
        self._carbon_transmission_cost_calculator = CarbonTransmissionCostCalculator(
            self._get_carbon_intensity_from_coordinates
        )

        self._carbon_intensity_cache: dict[tuple[float, float], float] = {}

    def retrieve_carbon_region_data(self) -> dict[str, dict[str, Any]]:
        result_dict: dict[str, dict[str, Any]] = {}
        for region_key, available_region in self._available_regions.items():
            latitude = available_region["latitude"]
            longitude = available_region["longitude"]
            carbon_intensity = self._get_carbon_intensity_from_coordinates(latitude, longitude)

            transmission_carbon_dict = {}

            for region_key_to, available_region_to in self._available_regions.items():
                (
                    intensity,
                    distance,
                ) = self._carbon_transmission_cost_calculator.calculate_transmission_carbon_intensity(  # pylint: disable=line-too-long
                    available_region, available_region_to
                )
                transmission_carbon_dict[region_key_to] = {
                    "carbon_intensity": intensity,
                    "distance": distance,
                    "unit": "gCO2eq/GB",
                }

            result_dict[region_key] = {
                "carbon_intensity": carbon_intensity,
                "unit": "gCO2eq/kWh",
                "transmission_carbon": transmission_carbon_dict,
            }
        return result_dict

    def _get_carbon_intensity_from_coordinates(self, latitude: float, longitude: float) -> float:
        if self._integration_test_on:
            return latitude + longitude

        if (latitude, longitude) in self._carbon_intensity_cache:
            return self._carbon_intensity_cache[(latitude, longitude)]
        electricitymaps = "https://api-access.electricitymaps.com/free-tier/carbon-intensity/latest?"

        if (datetime.datetime.now(GLOBAL_TIME_ZONE) - self._last_request).total_seconds() < self._request_backoff:
            time.sleep(self._request_backoff)

        if self._electricity_maps_auth_token is None:
            raise ValueError("ELECTRICITY_MAPS_AUTH_TOKEN environment variable not set")

        response = requests.get(
            electricitymaps + "lat=" + str(latitude) + "&lon=" + str(longitude),
            headers={"auth-token": self._electricity_maps_auth_token},
            timeout=10,
        )

        self._last_request = datetime.datetime.now(GLOBAL_TIME_ZONE)

        result = self._global_average_worst_case_carbon_intensity

        if response.status_code == 200:
            json_data = response.json()

            if "carbonIntensity" in json_data:
                result = json_data["carbonIntensity"]

        self._carbon_intensity_cache[(latitude, longitude)] = result
        return result
