import datetime
import json
import math
import time
from typing import Any

import boto3
import requests
from chalice import Chalice

app = Chalice(app_name="transmission_co2_tracker")
DEFAULT_REGION = "us-west-2"
AWS_DATACENTER_INFO_TABLE_NAME = "multi-x-serverless-datacenter-info"
WORLD_AVERAGE_CO2_INTENSITY = 475.0
TRANSMISSION_CO2_TABLE_NAME = "multi-x-serverless-transmission-co2"

SEGMENT_CACHE = {}

REQUEST_CACHE = {}

LAST_REQUEST = datetime.datetime.now()
REQUEST_THRESHOLD = 1
REQUEST_BACKOFF = 1


@app.schedule("rate(30 minutes)")
def index(event: Any) -> None:  # pylint: disable=unused-argument
    update_transmission_co2()


def update_transmission_co2() -> None:
    api_key = get_electricity_map_api_key()

    # Retrieve all regions and their coordinates
    client = boto3.client(
        "dynamodb",
        region_name=DEFAULT_REGION,
    )

    response = client.scan(
        TableName=AWS_DATACENTER_INFO_TABLE_NAME,
    )

    regions = []

    for item in response["Items"]:
        region = item["region_code"]["S"]
        provider = item["provider"]["S"]
        latitude = float(item["location"]["M"]["lat"]["N"])
        longitude = float(item["location"]["M"]["lng"]["N"])

        regions.append((region, provider, latitude, longitude))

    results = []

    for region1 in regions:
        for region2 in regions:
            if region1[0] == region2[0]:
                continue

            # Calculate the carbon intensity for the transmission line between the two regions
            carbon_intensity = calculate_transmission_cabron_coefficient_from_source_to_destination(
                (region1[2], region1[3]), (region2[2], region2[3]), api_key
            )

            # Upload the information with the current timestamp to DynamoDB
            results.append(
                {
                    "PutRequest": {
                        "Item": {
                            "region_from_to_codes": {
                                "S": region1[1] + ":" + region1[0] + ":" + region2[1] + ":" + region2[0]
                            },
                            "carbon_intensity": {"N": str(carbon_intensity)},
                            "timestamp": {
                                "S": get_current_time(),
                            },
                        }
                    }
                }
            )

    chunks = [results[i : i + 25] for i in range(0, len(results), 25)]

    for chunk in chunks:
        client.batch_write_item(RequestItems={TRANSMISSION_CO2_TABLE_NAME: chunk})


def get_current_time() -> str:
    time = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    return time


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


def calculate_transmission_cabron_coefficient_from_source_to_destination(
    source_location: tuple[float, float], destination_location: tuple[float, float], api_key: str
) -> float:
    cache_key = str(source_location) + str(destination_location)

    if cache_key in SEGMENT_CACHE:
        return SEGMENT_CACHE[cache_key]

    # Calculate the total distance in kilometers between source and destination
    total_distance = calculateDistanceInKMFromLatLong(source_location, destination_location)
    total_distance = int(total_distance)

    segments = []
    current_location = source_location
    current_distance = 0
    current_carbon_intensity = get_carbon_intensity_geo((current_location[0], current_location[1]), api_key)
    last_next_location = current_location
    segment_distance = 0

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
        next_carbon_intensity = get_carbon_intensity_geo((next_location[0], next_location[1]), api_key)

        if next_carbon_intensity != current_carbon_intensity:
            # Add the segment to the list when carbon intensity changes
            segments.append((current_location, last_next_location, segment_distance, current_carbon_intensity))
            current_carbon_intensity = next_carbon_intensity
            last_next_location = next_location
            current_location = next_location
            segment_distance = 0
        else:
            segment_distance += step_size
            last_next_location = next_location

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
    segments.append((current_location, last_next_location, segment_distance, current_carbon_intensity))

    # Calculate the average carbon intensity for the entire route by weighting each segment by its distance
    total_weighted_carbon_intensity = sum(segment[3] * (segment[2] / total_distance) for segment in segments)

    return total_weighted_carbon_intensity


def calculateDistanceInKMFromLatLong(source: tuple[float, float], dest: tuple[float, float]) -> int:
    R = 6371.0

    lat1 = math.radians(source[0])
    lon1 = math.radians(source[1])
    lat2 = math.radians(dest[0])
    lon2 = math.radians(dest[1])

    # Differences in latitude and longitude
    dlat = lat2 - lat1
    dlon = lon2 - lon1

    # Haversine formula
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    distance = R * c

    return distance


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
#     update_transmission_co2()
