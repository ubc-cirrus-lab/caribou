import datetime
import math
import os
import time
from typing import Any

import requests

from multi_x_serverless.data_collector.components.data_retriever import DataRetriever
from multi_x_serverless.deployment.common.remote_client.remote_client import RemoteClient
from multi_x_serverless.data_collector.components.carbon.carbon_transmission_cost_calculator.distance_carbon_transmission_cost_calculator import (
    DistanceCarbonTransmissionCostCalculator,
)
from multi_x_serverless.data_collector.components.carbon.carbon_transmission_cost_calculator.latency_carbon_transmission_cost_calculator import (
    LatencyCarbonTransmissionCostCalculator,
)


class CarbonRetriever(DataRetriever):
    def __init__(self, client: RemoteClient, config: dict) -> None:
        super().__init__(client)
        self._electricity_maps_auth_token = os.environ["ELECTRICITY_MAPS_AUTH_TOKEN"]
        self._request_backoff = 0.1
        self._last_request = datetime.datetime.now()
        self._global_average_worst_case_carbon_intensity = 475.0
        self._kwh_per_gb_estimate = 1.0
        if "carbon_transmission_cost_calculator" in config:
            if config["carbon_transmission_cost_calculator"] == "latency":
                self._carbon_transmission_cost_calculator = LatencyCarbonTransmissionCostCalculator(config, self._get_carbon_intensity_from_coordinates)
            elif config["carbon_transmission_cost_calculator"] == "distance":
                self._carbon_transmission_cost_calculator = DistanceCarbonTransmissionCostCalculator(config, self._get_carbon_intensity_from_coordinates)
            else:
                raise ValueError(
                    f"Invalid carbon transmission cost calculator: {config['carbon_transmission_cost_calculator']}"
                )
        else:
            self._carbon_transmission_cost_calculator = DistanceCarbonTransmissionCostCalculator(config, self._get_carbon_intensity_from_coordinates)

    def retrieve_carbon_region_data(self) -> dict[str, dict[str, Any]]:
        result_dict: dict[str, dict[str, Any]] = {}
        for region_key, available_region in self._available_regions.items():
            latitude = available_region["latitude"]
            longitude = available_region["longitude"]
            carbon_intensity = self._get_carbon_intensity_from_coordinates(latitude, longitude)

            transmission_carbon_dict = {}

            for region_key_to, available_region_to in self._available_regions.items():
                transmission_carbon_dict[region_key_to] = {
                    "carbon_intensity": self._carbon_transmission_cost_calculator.calculate_transmission_carbon_intensity(
                        available_region, available_region_to
                    ),
                    "unit": "gCO2eq/GB",
                }

            result_dict[region_key] = {
                "carbon_intensity": carbon_intensity,
                "unit": "gCO2eq/kWh",
                "transmission_carbon": transmission_carbon_dict,
            }
        return result_dict

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
