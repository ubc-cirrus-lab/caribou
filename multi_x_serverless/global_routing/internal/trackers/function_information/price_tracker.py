import os
import re

import requests
from bs4 import BeautifulSoup

GOOGLE_API_KEY = os.environ.get("MULTI_X_SERVERLESS_GOOGLE_API_KEY")
AWS_DATACENTER_INFO_TABLE_NAME = "multi-x-serverless-datacenter-info"


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
