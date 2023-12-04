import datetime
import json
from typing import Any

import boto3
import googlemaps
import requests
from bs4 import BeautifulSoup
from chalice import Chalice

app = Chalice(app_name="aws_datacenter_info")

AWS_DATACENTER_INFO_TABLE_NAME = "multi-x-serverless-datacenter-info"
DEFAULT_REGION = "us-west-2"

IGNORED_REGIONS = ["us-gov-west-1", "us-gov-east-1"]


@app.schedule("rate(10 days)")
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


def scrape_aws_locations(api_key: str) -> dict[str, tuple[float, float]]:
    url = "https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-regions-availability-zones.html#concepts-available-regions"  # pylint: disable=line-too-long
    response = requests.get(url, timeout=5)

    soup = BeautifulSoup(response.content, "html.parser")

    regions = {}

    tables = soup.find_all("table")

    if len(tables) == 0:
        raise ValueError("Could not find any tables on the AWS regions page")

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
            try:
                location = get_location(region_name, api_key)
            except ValueError:
                continue
            regions[region_code] = location
    return regions


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


def update_aws_datacenter_info(api_key: str) -> None:  # pylint: disable=too-many-locals
    """
    Updates the AWS datacenter info table in DynamoDB
    """
    client = boto3.client("pricing", region_name="us-east-1")  # Only available in us-east-1

    aws_locations = scrape_aws_locations(api_key)

    response = client.list_price_lists(
        ServiceCode="AWSLambda", EffectiveDate=datetime.datetime.now(), CurrencyCode="USD"
    )

    # TODO (vGsteiger): In a potential future version we would have to retrieve this information
    # from the price list of AmazonEC2 (also where we would get the VM information from)
    transmission_cost = 0.09
    free_data_egress_gb = 100000

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

        if invocation_call_sku_arm64 not in price_list["terms"]["OnDemand"]:
            print(region_code)
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
                    "free_data_egress_gb": {
                        "N": str(free_data_egress_gb),
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
                        "N": str(transmission_cost),
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
