import datetime
import json
from typing import Any, Optional

import boto3
import googlemaps
import Levenshtein
import requests
from bs4 import BeautifulSoup
from chalice import Chalice

app = Chalice(app_name="aws_datacenter_info")

AWS_DATACENTER_INFO_TABLE_NAME = "multi-x-serverless-datacenter-info"
DEFAULT_REGION = "us-west-2"

IGNORED_REGIONS = ["us-gov-west-1", "us-gov-east-1"]


# @app.schedule("rate(10 days)")
def scrape(event: Any) -> None:  # pylint: disable=unused-argument
    client = boto3.client(
        service_name="secretsmanager",
        region_name=DEFAULT_REGION,
    )
    secret_name = "multi-x-serverless/gcs/api-key"

    get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    secret = get_secret_value_response["SecretString"]

    secret = json.loads(secret)
    api_key = secret["MULTI_X_SERVERLESS_GOOGLE_API_KEY"]
    update_aws_datacenter_info(api_key)


def scrape_aws_locations(api_key: str) -> tuple[dict[str, tuple[float, float]], dict[str, str]]:
    url = "https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-regions-availability-zones.html#concepts-available-regions"  # pylint: disable=line-too-long
    response = requests.get(url, timeout=5)

    soup = BeautifulSoup(response.content, "html.parser")

    regions = {}

    tables = soup.find_all("table")

    if len(tables) == 0:
        raise ValueError("Could not find any tables on the AWS regions page")

    region_name_to_code = {}

    for table in tables:
        if not table.find_previous("h3").text.strip() == "Available Regions":
            continue

        rows = table.find_all("tr")

        for row in rows:
            cells = row.find_all("td")
            if len(cells) != 3:
                continue
            region_code = cells[0].text.strip()
            region_name = cells[1].text.strip()
            region_name_to_code[region_name] = region_code
            try:
                location = get_location(region_name, api_key)
            except ValueError:
                continue
            regions[region_code] = location
    return regions, region_name_to_code


def get_location(location_name: str, api_key: str) -> tuple[float, float]:
    gmaps = googlemaps.Client(key=api_key)

    if location_name == "Columbus":
        location_name = "Columbus, Ohio"  # Somehow Google Maps doesn't know where Columbus, OH is
    geocode_result = gmaps.geocode(location_name)
    if geocode_result:
        lat = geocode_result[0]["geometry"]["location"]["lat"]
        lng = geocode_result[0]["geometry"]["location"]["lng"]
    else:
        raise ValueError(f"Could not find location {location_name}")
    return lat, lng


def get_aws_product_skus(price_list: dict, client: boto3.client) -> tuple[str, str, str, str, str, str]:
    """
    Returns the product UIDs for the invocation and duration of a Lambda function

    architecture: "x86_64" or "arm64"
    price_list: price list from the AWS Pricing API
    """
    invocation_call_sku_arm64 = ""
    invocation_duration_sku_arm64 = ""
    invocation_call_sku_x86_64 = ""
    invocation_duration_sku_x86_64 = ""
    invocation_call_free_tier_sku = ""
    invocation_duration_free_tier_sku = ""

    price_list_arn = price_list["PriceListArn"]
    price_list_file = client.get_price_list_file_url(PriceListArn=price_list_arn, FileFormat="JSON")

    response = requests.get(price_list_file["Url"], timeout=5)
    price_list = response.json()

    for product in price_list["products"].values():
        if product["attributes"]["group"] == "AWS-Lambda-Requests-ARM" and product["attributes"]["location"] != "Any":
            invocation_call_sku_arm64 = product["sku"]
        if product["attributes"]["group"] == "AWS-Lambda-Duration-ARM" and product["attributes"]["location"] != "Any":
            invocation_duration_sku_arm64 = product["sku"]
        if product["attributes"]["group"] == "AWS-Lambda-Requests" and product["attributes"]["location"] != "Any":
            invocation_call_sku_x86_64 = product["sku"]
        if product["attributes"]["group"] == "AWS-Lambda-Duration" and product["attributes"]["location"] != "Any":
            invocation_duration_sku_x86_64 = product["sku"]
        if product["attributes"]["group"] == "AWS-Lambda-Requests" and product["attributes"]["location"] == "Any":
            invocation_call_free_tier_sku = product["sku"]
        if product["attributes"]["group"] == "AWS-Lambda-Duration" and product["attributes"]["location"] == "Any":
            invocation_duration_free_tier_sku = product["sku"]

    return (
        invocation_call_sku_arm64,
        invocation_duration_sku_arm64,
        invocation_call_sku_x86_64,
        invocation_duration_sku_x86_64,
        invocation_call_free_tier_sku,
        invocation_duration_free_tier_sku,
    )


def find_objects_with_pattern(data: dict, pattern: str) -> list[dict]:
    results = []

    def search_objects(obj: dict, parent: Optional[str] = None) -> None:
        for key, value in obj.items():
            if isinstance(value, dict):
                search_objects(value, key)
            elif isinstance(value, str) and pattern in value:
                results.append({"key": key, "value": value, "parent": parent})

    search_objects(data)

    return results


def fuzzy_match_region(original_destination_region: str, region_name_to_code: dict[str, str]) -> str:
    min_distance = 100
    destination_region = ""
    for region_name, inner_region_code in region_name_to_code.items():
        distance = Levenshtein.distance(original_destination_region, region_name)
        if distance < min_distance:
            min_distance = distance
            destination_region = inner_region_code
    return destination_region


def get_region_to_destination_transmission_cost(  # pylint: disable=too-many-locals
    transmission_cost_response: dict, client: boto3.client, region_name_to_code: dict
) -> dict:
    alternative_region_names: dict = {}
    region_to_destination_transmission_cost = {}
    for price_list in transmission_cost_response["PriceLists"]:
        region_code = price_list["RegionCode"]
        if region_code in IGNORED_REGIONS:
            continue

        price_list_arn = price_list["PriceListArn"]
        price_list_file = client.get_price_list_file_url(PriceListArn=price_list_arn, FileFormat="JSON")

        response = requests.get(price_list_file["Url"], timeout=5)
        price_list = response.json()

        price_list = price_list["terms"]["OnDemand"]

        pattern = "data transfer to"
        results = find_objects_with_pattern(price_list, pattern)

        region_to_transmissison_cost = {}
        for entry in results:
            destination_region = entry["value"].split(" - ")[-1].split(" data transfer to ")[1]
            price_str = entry["value"].split("$")[1].split(" ")[0]
            price = float(price_str)

            # The destination region is not always a region code, but sometimes a region name and
            # sometimes different than the one that we have in our dictionary
            if destination_region in region_name_to_code:
                cleaned_destination_region = region_name_to_code[destination_region]
            elif destination_region in alternative_region_names:  # pylint: disable=consider-using-get
                cleaned_destination_region = alternative_region_names[destination_region]
            else:
                # Use fuzzy matching to find the correct region code
                # Find the region code that has the smallest Levenshtein distance to the destination region
                cleaned_destination_region = fuzzy_match_region(destination_region, region_name_to_code)

            if cleaned_destination_region not in region_to_transmissison_cost:
                region_to_transmissison_cost[cleaned_destination_region] = price

        # For all regions that we don't have a transmission cost for, use the most common transmission cost
        max_arg = set(region_to_transmissison_cost.values())
        if len(max_arg) == 0:
            most_common_transmission_cost = (
                0.09  # Some regions are not covered by the way that we get the transmission cost
            )
        else:
            most_common_transmission_cost = max(max_arg, key=list(region_to_transmissison_cost.values()).count)

        for inner_region_code in region_name_to_code.values():
            if inner_region_code not in region_to_transmissison_cost:
                region_to_transmissison_cost[inner_region_code] = most_common_transmission_cost

        region_to_destination_transmission_cost[region_code] = region_to_transmissison_cost

    return region_to_destination_transmission_cost


def update_aws_datacenter_info(api_key: str) -> None:  # pylint: disable=too-many-locals
    """
    Updates the AWS datacenter info table in DynamoDB
    """
    client = boto3.client("pricing", region_name="us-east-1")  # Only available in us-east-1

    aws_locations, region_name_to_code = scrape_aws_locations(api_key)

    transmission_cost_response = client.list_price_lists(
        ServiceCode="AmazonEC2", EffectiveDate=datetime.datetime.now(), CurrencyCode="USD"
    )

    region_to_destination_transmission_cost = get_region_to_destination_transmission_cost(
        transmission_cost_response, client, region_name_to_code
    )

    response = client.list_price_lists(
        ServiceCode="AWSLambda", EffectiveDate=datetime.datetime.now(), CurrencyCode="USD"
    )

    results = []

    for price_list in response["PriceLists"]:
        region_code = price_list["RegionCode"]

        if region_code in IGNORED_REGIONS:
            continue
        (
            invocation_call_sku_arm64,
            invocation_duration_sku_arm64,
            invocation_call_sku_x86_64,
            invocation_duration_sku_x86_64,
            invocation_call_free_tier_sku,
            invocation_duration_free_tier_sku,
        ) = get_aws_product_skus(price_list, client)

        price_list_arn = price_list["PriceListArn"]
        price_list_file = client.get_price_list_file_url(PriceListArn=price_list_arn, FileFormat="JSON")

        response = requests.get(price_list_file["Url"], timeout=5)
        price_list = response.json()

        free_invocations_item = price_list["terms"]["OnDemand"][invocation_call_free_tier_sku][
            list(price_list["terms"]["OnDemand"][invocation_call_free_tier_sku].keys())[0]
        ]
        free_invocations = int(
            free_invocations_item["priceDimensions"][list(free_invocations_item["priceDimensions"].keys())[0]][
                "endRange"
            ]
        )  # in requests

        free_duration_item = price_list["terms"]["OnDemand"][invocation_duration_free_tier_sku][
            list(price_list["terms"]["OnDemand"][invocation_duration_free_tier_sku].keys())[0]
        ]
        free_compute_gb_s = int(
            free_duration_item["priceDimensions"][list(free_duration_item["priceDimensions"].keys())[0]]["endRange"]
        )  # in seconds

        invocation_cost_item_arm64 = price_list["terms"]["OnDemand"][invocation_call_sku_arm64][
            list(price_list["terms"]["OnDemand"][invocation_call_sku_arm64].keys())[0]
        ]
        invocation_cost_arm64 = float(
            invocation_cost_item_arm64["priceDimensions"][
                list(invocation_cost_item_arm64["priceDimensions"].keys())[0]
            ]["pricePerUnit"]["USD"]
        )

        invocation_cost_item_x86_64 = price_list["terms"]["OnDemand"][invocation_call_sku_x86_64][
            list(price_list["terms"]["OnDemand"][invocation_call_sku_x86_64].keys())[0]
        ]
        invocation_cost_x86_64 = float(
            invocation_cost_item_x86_64["priceDimensions"][
                list(invocation_cost_item_x86_64["priceDimensions"].keys())[0]
            ]["pricePerUnit"]["USD"]
        )

        compute_cost_item_sku_arm64 = price_list["terms"]["OnDemand"][invocation_duration_sku_arm64][
            list(price_list["terms"]["OnDemand"][invocation_duration_sku_arm64].keys())[0]
        ]

        compute_cost_arm64 = compute_cost_item_sku_arm64["priceDimensions"]

        compute_cost_item_sku_x86_64 = price_list["terms"]["OnDemand"][invocation_duration_sku_x86_64][
            list(price_list["terms"]["OnDemand"][invocation_duration_sku_x86_64].keys())[0]
        ]

        compute_cost_x86_64 = compute_cost_item_sku_x86_64["priceDimensions"]

        location = aws_locations[region_code]

        compute_cost_arm64_with_unit = get_compute_cost_with_unit(compute_cost_arm64)

        compute_cost_x86_64_with_unit = get_compute_cost_with_unit(compute_cost_x86_64)

        transmission_cost_gb = {}
        for region, price in region_to_destination_transmission_cost[region_code].items():
            transmission_cost_gb[region] = {"N": str(price)}

        item = {
            "PutRequest": {
                "Item": {
                    "region_code": {
                        "S": region_code,
                    },
                    "provider": {
                        "S": "aws",
                    },
                    "price_last_arn": {
                        "S": price_list_arn,
                    },
                    "last_updated": {
                        "S": str(datetime.datetime.now()),
                    },
                    "free_invocations": {
                        "N": str(free_invocations),
                    },
                    "free_compute_gb_s": {
                        "N": str(free_compute_gb_s),
                    },
                    "invocation_cost_arm64": {
                        "N": str(invocation_cost_arm64),
                    },
                    "invocation_cost_x86_64": {
                        "N": str(invocation_cost_x86_64),
                    },
                    "compute_cost_arm64_gb_s": {
                        "L": compute_cost_arm64_with_unit,
                    },
                    "compute_cost_x86_64_gb_s": {
                        "L": compute_cost_x86_64_with_unit,
                    },
                    "transmission_cost_gb": {
                        "M": transmission_cost_gb,
                    },
                    "cfe": {
                        "N": str(0.92),  # This is an estimate
                    },
                    "pue": {
                        "N": str(1.2),  # This is an estimate
                    },
                    "location": {
                        "M": {
                            "lat": {
                                "N": str(location[0]),
                            },
                            "lng": {
                                "N": str(location[1]),
                            },
                        },
                    },
                }
            }
        }

        results.append(item)

    write_results(results, AWS_DATACENTER_INFO_TABLE_NAME)


def get_compute_cost_with_unit(compute_cost: dict) -> list[dict]:
    result = []
    for value in compute_cost.values():
        result.append(
            {
                "M": {
                    "beginRange": {
                        "N": str(value["beginRange"]),
                    },
                    "pricePerUnit": {
                        "N": str(value["pricePerUnit"]["USD"]),
                    },
                }
            }
        )
    return result


def write_results(results: list[dict], table_name: str) -> None:
    client = boto3.client(
        "dynamodb",
        region_name=DEFAULT_REGION,
    )

    # Split the results into chunks of 25 items
    chunks = [results[i : i + 25] for i in range(0, len(results), 25)]

    for chunk in chunks:
        client.batch_write_item(RequestItems={table_name: chunk})


if __name__ == "__main__":
    scrape(None)
