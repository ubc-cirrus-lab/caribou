import datetime
import json
import time
from typing import Any

import boto3
import requests
from chalice import Chalice

app = Chalice(app_name="grid_co2_tracker")
DEFAULT_REGION = "us-west-2"
WORLD_AVERAGE_CO2_INTENSITY = 475.0
AWS_DATACENTER_INFO_TABLE_NAME = "multi-x-serverless-datacenter-info"
DATACENTER_GRID_CO2_TABLE_NAME = "multi-x-serverless-datacenter-grid-co2"

REQUEST_CACHE: dict = {}
LAST_REQUEST = datetime.datetime.now()
REQUEST_THRESHOLD = 1
REQUEST_BACKOFF = 1


@app.schedule("rate(30 minutes)")
def index(event: Any) -> None:  # pylint: disable=unused-argument
    update_datacenter_grid_co2()


def get_electricity_map_api_key() -> str:
    client = boto3.client(
        service_name="secretsmanager",
        region_name=DEFAULT_REGION,
    )
    secret_name = "multi-x-serverless/electricity-map/api-key"

    get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    secret = get_secret_value_response["SecretString"]

    secret = json.loads(secret)
    api_key = secret["ELECTRICITY_MAPS_API_KEY"]
    return api_key


def update_datacenter_grid_co2() -> None:
    api_key = get_electricity_map_api_key()

    # Retrieve all regions and their coordinates
    client = boto3.client(
        "dynamodb",
        region_name=DEFAULT_REGION,
    )

    response = client.scan(
        TableName=AWS_DATACENTER_INFO_TABLE_NAME,
    )

    results = []

    for item in response["Items"]:
        region = item["region_code"]["S"]
        provider = item["provider"]["S"]
        latitude = float(item["location"]["M"]["lat"]["N"])
        longitude = float(item["location"]["M"]["lng"]["N"])

        # Get carbon intensity
        carbon_intensity = get_carbon_intensity_geo((latitude, longitude), api_key)

        # Upload the information with the current timestamp to DynamoDB
        results.append(
            {
                "PutRequest": {
                    "Item": {
                        "region_code_provider": {"S": region + ":" + provider},
                        "timestamp": {
                            "S": get_current_time(),
                        },
                        "carbon_intensity": {"N": str(carbon_intensity)},
                    }
                }
            }
        )

    # Split the results into chunks of 25 items
    chunks = [results[i: i + 25] for i in range(0, len(results), 25)]

    for chunk in chunks:
        client.batch_write_item(RequestItems={DATACENTER_GRID_CO2_TABLE_NAME: chunk})


def get_current_time() -> str:
    time = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    return time


def get_carbon_intensity_geo(location: tuple[float, float], api_key: str) -> float:
    """
    Returns the carbon intensity of the grid at a given location in gCO2eq/kWh
    """
    cache_key = str(location[0]) + str(location[1])
    if cache_key in REQUEST_CACHE:
        return REQUEST_CACHE[cache_key]
    global LAST_REQUEST
    url = "https://api-access.electricitymaps.com/free-tier/carbon-intensity/latest?"

    if (datetime.datetime.now() - LAST_REQUEST).total_seconds() < REQUEST_THRESHOLD:
        time.sleep(REQUEST_BACKOFF)

    r = requests.get(
        url + "lat=" + str(location[0]) + "&lon=" + str(location[1]),
        headers={"auth-token": api_key},
        timeout=5,
    )

    LAST_REQUEST = datetime.datetime.now()

    if r.status_code == 200:
        json_data = r.json()

        if "carbonIntensity" not in json_data:
            print(
                f"Error getting carbon intensity from Electricity Maps API: {json_data} for {location[0]}, {location[1]}"  # pylint: disable=line-too-long
            )
            raise RuntimeError("Error getting carbon intensity from Electricity Maps API, no carbon intensity")

        REQUEST_CACHE[cache_key] = json_data["carbonIntensity"]
        return json_data["carbonIntensity"]

    if r.status_code == 404 and "No recent data for zone" in r.text:
        REQUEST_CACHE[cache_key] = WORLD_AVERAGE_CO2_INTENSITY
        return WORLD_AVERAGE_CO2_INTENSITY

    print(
        f"Error getting carbon intensity from Electricity Maps API: {r.status_code} for {location[0]}, {location[1]}"  # pylint: disable=line-too-long
    )
    raise RuntimeError("Error getting carbon intensity from Electricity Maps API, status code: " + str(r.status_code))


# if __name__ == "__main__":
#     update_datacenter_grid_co2()
