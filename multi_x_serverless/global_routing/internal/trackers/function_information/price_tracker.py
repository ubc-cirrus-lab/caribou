import os
import re

import requests
from bs4 import BeautifulSoup

from multi_x_serverless.global_routing.internal.trackers.shared import get_item_from_dynamodb

GOOGLE_API_KEY = os.environ.get("MULTI_X_SERVERLESS_GOOGLE_API_KEY")
AWS_DATACENTER_INFO_TABLE_NAME = "multi-x-serverless-datacenter-info"


def calculate_aws_compute_cost(
    price_dimensions: dict, estimated_gb_seconds_per_month: float, compute_free_tier: float
) -> float:
    compute_cost = 0.0

    if estimated_gb_seconds_per_month <= compute_free_tier:
        return compute_cost

    estimated_gb_seconds_per_month -= compute_free_tier

    for price_dimension in price_dimensions.values():
        if estimated_gb_seconds_per_month <= int(price_dimension["endRange"]):
            compute_cost += float(price_dimension["pricePerUnit"]["USD"]) * estimated_gb_seconds_per_month
            break
        compute_cost += float(price_dimension["pricePerUnit"]["USD"]) * int(price_dimension["endRange"])
        estimated_gb_seconds_per_month -= int(price_dimension["endRange"])
    return compute_cost


def get_aws_price_for_function(  # pylint: disable=too-many-locals
    region_code: str,
    estimated_memory: int,
    estimated_duration: int,
    estimated_number_of_requests_per_month: int,
    architecture: str,
    estimated_data_egress: float,
) -> float:
    """
    Returns the price for a function in a given region in USD

    estimated_memory: in MB
    estimated_duration: in ms
    estimated_number_of_requests_per_month: in number of requests
    architecture: "arm64" or "x86_64"
    estimated_data_egress: in MB
    """
    stored_aws_data = get_item_from_dynamodb({"region_code": region_code}, AWS_DATACENTER_INFO_TABLE_NAME)
    if stored_aws_data:
        stored_aws_data = stored_aws_data["data"]
        free_invocations = int(stored_aws_data["free_invocations"]["N"])
        free_compute_gb_s = int(stored_aws_data["free_compute_gb_s"]["N"])
        free_data_egress_gb = int(stored_aws_data["free_data_egress_gb"]["N"])

        transmission_cost_gb = float(stored_aws_data["transmission_cost_gb"]["N"])
        if estimated_data_egress > free_data_egress_gb:
            transmission_cost = ((estimated_data_egress / 1000) - free_data_egress_gb) * transmission_cost_gb
        else:
            transmission_cost = 0

        invocation_cost_gb = float(stored_aws_data["invocation_cost_" + architecture + "_gb_s"]["N"])
        if estimated_number_of_requests_per_month > free_invocations:
            invocation_cost = invocation_cost_gb * (
                (estimated_number_of_requests_per_month - free_invocations) / 1000000
            )
        else:
            invocation_cost = 0

        estimated_gb_seconds_per_month = (estimated_memory * estimated_duration) / 1000000
        compute_cost = calculate_aws_compute_cost(
            stored_aws_data["compute_cost_" + architecture], estimated_gb_seconds_per_month, free_compute_gb_s
        )

        return invocation_cost + compute_cost + transmission_cost

    raise ValueError(f"Could not find data for region {region_code}")


def get_gcp_price_for_function(  # pylint: disable=too-many-branches,too-many-statements,too-many-locals
    tier: int,
    estimated_memory: int,
    estimated_v_cpu: float,
    estimated_duration: int,
    estimated_number_of_requests_per_month: int,
    estimated_data_egress: float,
) -> float:
    """
    Returns the price for a function in a given region in USD

    tier: 1 or 2
    estimated_memory: in MB
    estimated_v_cpu: in vCPU
    estimated_duration: in ms
    estimated_number_of_requests_per_month: in number of requests
    estimated_data_egress: in MB
    """
    # TODO (vGsteiger): Change to use the API
    api_call = "https://cloudbilling.googleapis.com/v2beta/skus"

    response = requests.get(
        api_call,
        params={
            "key": GOOGLE_API_KEY,
        },
        timeout=5,
    )

    url = "https://cloud.google.com/functions/pricing"
    response = requests.get(url, timeout=5)

    soup = BeautifulSoup(response.content, "html.parser")

    tables = soup.find_all("table")
    invocation_price = 0.0
    compute_price = 0.0
    networking_price = 0.0
    free_gb_seconds = 400000
    free_ghz_seconds = 200000
    free_egress_data = 5
    free_invocations = 2000000
    vcpu_to_ghz_conversion = 2.4
    data_transmission_per_gb = 0.12

    for table in tables:
        rows = table.find_all("tr")

        if table.find_previous("h3").text == "Invocations":
            for row in rows:
                columns = row.find_all("td")
                if len(columns) == 2:
                    invocations = columns[0].text.strip()
                    price = columns[1].text.strip()
                    if "Beyond" in invocations:
                        free_invocations = int(invocations.split(" ")[1])
                        price_per_million_invocations = float(price[1:])  # Remove dollar sign
                    free_invocations = free_invocations * 1000000
            if estimated_number_of_requests_per_month > free_invocations:
                invocation_price = price_per_million_invocations * (
                    (estimated_number_of_requests_per_month - free_invocations) / 1000000
                )
        elif table.find_previous("h3").text == "Compute Time" and table.find("th").text == "Unit":
            rows = table.find_all("tr")
            for row in rows:
                columns = row.find_all("td")
                if len(columns) == 3:
                    if columns[0].text.strip() == "GB-Second":
                        tier1_gb_seconds = float(columns[1].text.strip().split("$")[1].split("\n")[0])
                        tier2_gb_seconds = float(columns[2].text.strip().split("$")[1].split("\n")[0])
                    elif columns[0].text.strip() == "GHz-Second":
                        tier1_ghz_seconds = float(columns[1].text.strip().split("$")[1].split("\n")[0])
                        tier2_ghz_seconds = float(columns[2].text.strip().split("$")[1].split("\n")[0])

            memory_gb_seconds_price = tier1_gb_seconds if tier == 1 else tier2_gb_seconds
            vcpu_ghz_seconds_price = tier1_ghz_seconds if tier == 1 else tier2_ghz_seconds

            free_tier_title = table.find_next("h4", string="Free Tier")
            free_tier_text = free_tier_title.find_next("p").text.strip()

            pattern_gb_seconds = re.compile(r"(\d{1,3}(?:,\d{3})*)(?=\s*GB-seconds)")
            pattern_ghz_seconds = re.compile(r"(\d{1,3}(?:,\d{3})*)(?=\s*GHz-seconds)")

            free_gb_seconds_match = pattern_gb_seconds.search(free_tier_text)
            free_gb_seconds = int(free_gb_seconds_match.group().replace(",", "")) if free_gb_seconds_match else 0

            memory_gb_seconds = estimated_memory * (estimated_duration / 1000)

            if memory_gb_seconds > free_gb_seconds:
                compute_price += (memory_gb_seconds - free_gb_seconds) * memory_gb_seconds_price

            free_ghz_seconds_match = pattern_ghz_seconds.search(free_tier_text)
            free_ghz_seconds = int(free_ghz_seconds_match.group().replace(",", "")) if free_ghz_seconds_match else 0

            vcpu_ghz_seconds = estimated_v_cpu * vcpu_to_ghz_conversion * (estimated_duration / 1000)

            if vcpu_ghz_seconds > free_ghz_seconds:
                compute_price += (vcpu_ghz_seconds - free_ghz_seconds) * vcpu_ghz_seconds_price
        elif table.find_previous("h3").text == "Networking":
            rows = table.find_all("tr")
            for row in rows:
                columns = row.find_all("td")
                if len(columns) == 2:
                    if columns[0].text.strip() == "Outbound Data (Egress)":
                        data_transmission_per_gb = float(columns[1].text.strip().split("$")[1])
                    elif columns[0].text.strip() == "Outbound Data per month":
                        free_egress_data = int(columns[1].text.strip().split("GB")[0])

            estimated_egress_data = (estimated_data_egress / 1000) * estimated_number_of_requests_per_month
            if estimated_egress_data > free_egress_data:
                networking_price += (estimated_egress_data - free_egress_data) * data_transmission_per_gb

    return invocation_price + compute_price + networking_price


def get_price_for_function(
    region_code: str,
    tier: int,
    provider: str,
    estimated_memory: int,
    estimated_v_cpu: float,
    estimated_duration: int,
    estimated_number_of_requests_per_month: int,
    architecture: str,
    estimated_data_egress: float,
) -> float:
    """
    Returns the price for a function in a given region in USD

    tier: 1 or 2
    provider: "aws" or "gcp"
    estimated_memory: in MB
    estimated_v_cpu: in vCPU
    estimated_duration: in ms
    estimated_number_of_requests_per_month: in number of requests
    estimated_data_egress: in MB
    """
    pricing = 0.0
    if provider == "aws":
        pricing = get_aws_price_for_function(
            region_code,
            estimated_memory,
            estimated_duration,
            estimated_number_of_requests_per_month,
            architecture,
            estimated_data_egress,
        )
    elif provider == "gcp":
        pricing = get_gcp_price_for_function(
            tier,
            estimated_memory,
            estimated_v_cpu,
            estimated_duration,
            estimated_number_of_requests_per_month,
            estimated_data_egress,
        )
    else:
        raise ValueError(f"Provider {provider} is not supported.")
    return pricing


print(get_price_for_function("us-east-1", 1, "aws", 128, 0.25, 100, 1000000, "x86_64", 0))
