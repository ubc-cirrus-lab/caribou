import asyncio
import math
import os
import time
from datetime import datetime, timedelta, timezone
from functools import partial
from os import mkdir
from pathlib import Path
from typing import Any, Callable, Optional

import pandas as pd
import requests
from statsmodels.tsa.holtwinters import ExponentialSmoothing

from caribou.common.constants import GLOBAL_TIME_ZONE
from caribou.common.models.remote_client.remote_client import RemoteClient
from caribou.common.utils import str_to_bool
from caribou.data_collector.components.data_retriever import DataRetriever
from caribou.data_collector.utils.constants import EC_MAPS_HISTORICAL_BASE_URL
from caribou.data_collector.utils.ec_maps_zone_finder.index import find_zone as finder


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

        self._carbon_intensity_history_cache: dict[tuple[float, float], Optional[dict[str, Any]]] = {}

        self._carbon_intensity_cache: dict[tuple[float, float], float] = {}

        self._this_file_dir = Path(__file__).resolve().parent
        self._project_root = self._this_file_dir.parent.parent.parent
        self._finder_data_path = self._project_root / "data_collector" / "utils" / "ec_maps_zone_finder"
        self._finder_data_csv_path = self._finder_data_path / "data.csv"
        self._finder_data_csv_path.parent.mkdir(parents=True, exist_ok=True)

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

            if overall_average_data is None:
                continue

            ## Hourly average carbon intensity
            # For this we need a 24 hour loop
            hourly_average_data: dict[str, dict[str, Any]] = {}
            for hour in range(24):
                carbon_intensity = self._get_execution_carbon_intensity(
                    available_region, partial(self._get_hour_average_carbon_intensity, hour=hour)
                )

                if carbon_intensity is not None:
                    hourly_average_data[str(hour)] = carbon_intensity

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
    ) -> Optional[dict[str, Any]]:
        latitude = available_region["latitude"]
        longitude = available_region["longitude"]

        carbon_intensity = get_carbon_intensity_from_coordinates(latitude, longitude)

        if carbon_intensity is None:
            return None

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

    def _get_hour_average_carbon_intensity(self, latitude: float, longitude: float, hour: int) -> Optional[float]:
        if self._integration_test_on:
            # Return the absolute value of the sum of the latitude and longitude
            return abs(latitude + longitude)

        carbon_information = self._get_carbon_intensity_information(latitude, longitude)

        if carbon_information is None:
            return None

        return carbon_information["hourly_average"].get(hour, self._global_average_worst_case_carbon_intensity)

    def _get_overall_average_carbon_intensity(self, latitude: float, longitude: float) -> Optional[float]:
        if self._integration_test_on:
            return abs(latitude + longitude)

        carbon_information = self._get_carbon_intensity_information(latitude, longitude)

        if carbon_information is None:
            return None

        return carbon_information["overall_average"]

    def _get_carbon_intensity_information(self, latitude: float, longitude: float) -> Optional[dict[str, Any]]:
        # Check cache if the carbon intensity is already there
        if (latitude, longitude) in self._carbon_intensity_history_cache:
            return self._carbon_intensity_history_cache[(latitude, longitude)]

        # If not, retrieve then process the raw carbon intensity history
        raw_carbon_intensity_history = self._get_raw_carbon_intensity_history_range(
            latitude, longitude, self._start_timestamp, self._end_timestamp
        )

        if len(raw_carbon_intensity_history) == 0:
            self._carbon_intensity_history_cache[(latitude, longitude)] = None
            return None

        processed_carbon_intensity = self._process_raw_carbon_intensity_history(raw_carbon_intensity_history)
        self._carbon_intensity_history_cache[(latitude, longitude)] = processed_carbon_intensity

        return processed_carbon_intensity

    def _process_raw_carbon_intensity_history(
        self, raw_carbon_intensity_history: list[dict[str, str]]
    ) -> dict[str, Any]:
        # Sorting the data by datetime to ensure chronological order
        sorted_data = sorted(raw_carbon_intensity_history, key=lambda x: x["datetime"])

        # Extracting just the carbon intensity values for time series forecasting
        carbon_values = []
        for entry in sorted_data:
            if "carbonIntensity" in entry and entry["carbonIntensity"] is not None:
                carbon_values.append(float(entry["carbonIntensity"]))

        first_prediction_hour = datetime.fromisoformat(sorted_data[-1]["datetime"].replace("Z", "")) + timedelta(
            hours=1
        )

        model = ExponentialSmoothing(carbon_values, trend=None, seasonal="additive", seasonal_periods=24).fit()

        y_pred = model.forecast(24)

        # For loop from 0 to 23, just a loop in python
        hourly_avg = {}
        for i in range(24):
            future_time = first_prediction_hour + timedelta(hours=i)
            hourly_avg[future_time.hour] = y_pred[i]

        average_pred_carbon_intensity = sum(y_pred) / len(y_pred)

        return {
            "overall_average": average_pred_carbon_intensity,
            "hourly_average": hourly_avg,
        }

    def _get_raw_carbon_intensity_history_range(
        self, latitude: float, longitude: float, start_timestamp: str, end_timestamp: str
    ) -> list[dict[str, str]]:
        electricitymaps = "https://api.electricitymap.org/v3/carbon-intensity/past-range?"

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

        else:
            ecmaps_zone = asyncio.run(self._get_ecmaps_zone_from_coordinates(latitude, longitude))

            if not isinstance(ecmaps_zone, str) or not ecmaps_zone:
                print(f"Fallback: Failed to obtain a valid zone {ecmaps_zone}")
                return []

            self._get_ec_maps_historical_carbon_intensity_csv(ecmaps_zone)
            return self._get_co2_historical_json(ecmaps_zone)

        return result

    def _get_carbon_intensity_from_coordinates(self, latitude: float, longitude: float) -> float:
        # This is a legacy function, for now it is not used, it is here as it may be used in the future
        if self._integration_test_on:
            return latitude + longitude

        if (latitude, longitude) in self._carbon_intensity_cache:
            return self._carbon_intensity_cache[(latitude, longitude)]
        electricitymaps = "https://api.electricitymap.com/v3/carbon-intensity/latest?"

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

    async def _get_ecmaps_zone_from_coordinates(self, latitude: float, longitude: float) -> str | None:
        header = "lon,lat,zone"
        results = [header]
        results.append(f"{longitude},{latitude},")
        zone_csv = self._finder_data_csv_path
        with open(zone_csv, "w", encoding="utf-8") as file:
            file.write("\n".join(results) + "\n")

        await finder()

        with open(zone_csv, "r", encoding="utf-8") as file:
            lines = file.read().strip().split("\n")
            rows = lines[1:]
            row = rows[0]
            _, _, zone = row.split(",")
        return zone

    def _get_ec_maps_historical_carbon_intensity_csv(self, zone: str) -> None:
        url = EC_MAPS_HISTORICAL_BASE_URL + zone + "_2024_hourly.csv"
        filename = self._finder_data_path / "hourly_data" / url.split("/")[-1]
        hourly_path = self._finder_data_path / "hourly_data"
        if not hourly_path.exists():
            mkdir(hourly_path)

        with requests.get(url, timeout=10, stream=True) as response:
            response.raise_for_status()

            with open(filename, "wb") as out_file:
                for chunk in response.iter_content(chunk_size=8192):
                    out_file.write(chunk)

    def _get_co2_historical_json(self, zone: str) -> list[dict[str, str]]:
        my_file = Path(f"{self._finder_data_path}/hourly_data/{zone}_2024_hourly.csv")
        if not my_file.is_file():
            self._get_ec_maps_historical_carbon_intensity_csv(zone)

        data = pd.read_csv(
            f"{self._finder_data_path}/hourly_data/{zone}_2024_hourly.csv", dtype={"Datetime (UTC)": str}
        )

        datetime_col_name = "Datetime (UTC)"
        carbon_intensity_col_name = "Carbon intensity gCO₂eq/kWh (Life cycle)"
        selected = data[[datetime_col_name, carbon_intensity_col_name]].copy()

        selected[datetime_col_name] = pd.to_datetime(
            selected[datetime_col_name], errors="coerce", format="%Y-%m-%d %H:%M:%S"
        )

        if selected["Datetime (UTC)"].dt.tz is None:
            selected["Datetime (UTC)"] = selected["Datetime (UTC)"].dt.tz_localize(
                "UTC", ambiguous="infer", nonexistent="shift_forward"
            )
        else:
            selected["Datetime (UTC)"] = selected["Datetime (UTC)"].dt.tz_convert("UTC")

        start_date = datetime.now(timezone.utc) - timedelta(days=365)
        start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = start_date + timedelta(days=7)

        in_one_week = selected[
            (selected["Datetime (UTC)"] > start_date) & (selected["Datetime (UTC)"] < end_date)
        ].copy()

        raw_carbon_intensity_history_list = []

        for _, row in in_one_week.iterrows():
            datetime_utc = row["Datetime (UTC)"]
            carbon_intensity_value = row["Carbon intensity gCO₂eq/kWh (Life cycle)"]

            datetime_str = datetime_utc.isoformat().replace("+00:00", "Z")

            carbon_intensity_str = str(carbon_intensity_value)

            raw_carbon_intensity_history_list.append(
                {"datetime": datetime_str, "carbonIntensity": carbon_intensity_str}
            )

        return raw_carbon_intensity_history_list
