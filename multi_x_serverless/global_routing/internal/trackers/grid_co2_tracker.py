import os

import requests

from multi_x_serverless.shared.classes import Location
from multi_x_serverless.shared.remote_logging import Logger, get_logger

ELECTRICITY_MAPS_API_KEY = os.environ.get("ELECTRICITY_MAPS_API_KEY")
WORLD_AVERAGE_CO2_INTENSITY = 475.0

with get_logger(__name__) as logger:

    def get_carbon_intensity_geo(location: Location, logger: Logger = logger) -> float:
        """
        Returns the carbon intensity of the grid at a given location in gCO2eq/kWh
        """
        url = "https://api-access.electricitymaps.com/free-tier/carbon-intensity/latest?"

        r = requests.get(
            url + "lon=" + str(location.get_longitude()) + "&lat=" + str(location.get_latitude()),
            headers={"auth-token": ELECTRICITY_MAPS_API_KEY},
            timeout=5,
        )

        if r.status_code == 200:
            json_data = r.json()
            if "error" in json_data and "No recent data for zone" in json_data["error"]:
                logger.info(f"No recent data for zone {location.get_longitude()}, {location.get_latitude()}")
                return WORLD_AVERAGE_CO2_INTENSITY

            if "carbonIntensity" not in json_data:
                logger.error(
                    f"Error getting carbon intensity from Electricity Maps API: {json_data} for {location.get_longitude()}, {location.get_latitude()}"  # pylint: disable=line-too-long
                )
                raise RuntimeError("Error getting carbon intensity from Electricity Maps API, no carbon intensity")

            return json_data["carbonIntensity"]

        logger.error(
            f"Error getting carbon intensity from Electricity Maps API: {r.status_code} for {location.get_longitude()}, {location.get_latitude()}"  # pylint: disable=line-too-long
        )
        raise RuntimeError(
            "Error getting carbon intensity from Electricity Maps API, status code: " + str(r.status_code)
        )
