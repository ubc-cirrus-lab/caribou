import datetime

import boto3
import requests
from boto3.session import Config
from bs4 import BeautifulSoup

from multi_x_serverless.global_routing.internal.trackers.constants import AWS_DATACENTER_INFO_TABLE_NAME
from multi_x_serverless.global_routing.internal.trackers.shared import create_aws_table_if_not_exists, write_results
from multi_x_serverless.shared.classes import Location, get_location


def create_aws_datacenter_info_table_if_not_exists():
    table_name = AWS_DATACENTER_INFO_TABLE_NAME
    key_schema = [
        {"AttributeName": "region_code", "KeyType": "HASH"},
    ]

    attribute_definitions = [
        {"AttributeName": "region_code", "AttributeType": "S"},
    ]

    provisioned_throughput = {"ReadCapacityUnits": 10, "WriteCapacityUnits": 10}

    create_aws_table_if_not_exists(table_name, key_schema, attribute_definitions, provisioned_throughput)


def scrape_aws_locations() -> dict[str, Location]:
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
            region_code = cells[0].text.strip()
            region_name = cells[1].text.strip()
            try:
                location = get_location(region_name)
            except ValueError:
                continue
            regions[region_code] = location
    return regions


def get_aws_product_skus(price_list: dict) -> tuple[str, str, str, str, str, str]:
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


def update_aws_datacenter_info() -> None:  # pylint: disable=too-many-locals
    """
    Updates the AWS datacenter info table in DynamoDB
    """
    create_aws_datacenter_info_table_if_not_exists()

    price_config = Config(region_name="us-west-2", signature_version="v4", retries={"max_attempts": 10})

    client = boto3.client("pricing", config=price_config)

    response = client.list_price_lists(
        ServiceCode="AWSLambda", EffectiveDate=datetime.datetime.now(), CurrencyCode="USD"
    )

    (
        invocation_call_sku_arm64,
        invocation_duration_sku_arm64,
        invocation_call_sku_x86_64,
        invocation_duration_sku_x86_64,
        invocation_call_free_tier_sku,
        invocation_duration_free_tier_sku,
    ) = get_aws_product_skus(response["PriceLists"][0])

    aws_locations = scrape_aws_locations()

    # TODO (vGsteiger): In a potential future version we would have to retrieve this information
    # from the price list of AmazonEC2 (also where we would get the VM information from)
    transmission_cost = 0.09
    free_data_egress_gb = 100000

    results = []

    for price_list in response["PriceLists"]:
        region_code = price_list["RegionCode"]
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

        item = {
            "region_code": {"S": region_code},
            "data": {
                "price_last_arn": {"S": price_list_arn},
                "last_updated": {"S": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"},
                "free_invocations": {"N": str(free_invocations)},
                "free_compute_gb_s": {"N": str(free_compute_gb_s)},
                "free_data_egress_gb": {"N": str(free_data_egress_gb)},
                "invocation_cost_arm64": {"N": str(invocation_cost_arm64)},
                "invocation_cost_x86_64": {"N": str(invocation_cost_x86_64)},
                "compute_cost_arm64_gb_s": {"M": compute_cost_arm64},
                "compute_cost_x86_64_gb_s": {"M": compute_cost_x86_64},
                "transmission_cost_gb": {"N": str(transmission_cost)},
                "location": {
                    "M": {
                        "latitude": {"N": str(location.get_latitude())},
                        "longitude": {"N": str(location.get_longitude())},
                    }
                },
            },
        }

        results.append(item)

    write_results(results, AWS_DATACENTER_INFO_TABLE_NAME)


def main():
    update_aws_datacenter_info()
