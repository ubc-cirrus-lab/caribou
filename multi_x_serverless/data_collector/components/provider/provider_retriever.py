import datetime
import json
import os
from typing import Any, Optional

import boto3
import googlemaps
import requests
from bs4 import BeautifulSoup

from multi_x_serverless.common.provider import Provider
from multi_x_serverless.data_collector.components.data_retriever import DataRetriever


class ProviderRetriever(DataRetriever):
    def __init__(self) -> None:
        self._google_api_key = os.environ.get("GOOGLE_API_KEY")
        self._aws_pricing_client = boto3.client("pricing", region_name="us-east-1")
        self._aws_region_name_to_code: dict[str, str] = {}

    def retrieve_aws_regions(self) -> dict[str, dict[str, Any]]:
        amazon_region_url = "https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-regions-availability-zones.html#concepts-available-regions"  # pylint: disable=line-too-long
        amazon_region_page = requests.get(amazon_region_url)

        amazon_region_page_soup = BeautifulSoup(amazon_region_page.content, "html.parser")

        regions = {}

        tables = amazon_region_page_soup.find_all("table")

        if len(tables) == 0:
            raise ValueError("Could not find any tables on the AWS regions page")

        for table in tables:
            if not table.find_previous("h3").text.strip() == "Available Regions":
                continue

            table_rows = table.find_all("tr")

            for table_row in table_rows:
                table_cells = table_row.find_all("td")
                if len(table_cells) != 3:
                    continue
                region_code = table_cells[0].text.strip()
                region_name = table_cells[1].text.strip()
                coordinates = self.retrieve_location(region_name)
                regions[f"{Provider.AWS.value}:{region_code}"] = {
                    "name": region_name,
                    "provider": Provider.AWS.value,
                    "code": region_code,
                    "latitude": coordinates[0],
                    "longitude": coordinates[1],
                }
                self._aws_region_name_to_code[region_name] = region_code

        return regions

    def retrieve_location(self, name: str) -> tuple[float, float]:
        google_maps = googlemaps.Client(key=self._google_api_key)

        if name == "Columbus":
            name = "Columbus, Ohio"  # Somehow Google Maps doesn't know where Columbus, OH is
        geocode_result = google_maps.geocode(name)
        if geocode_result:
            latitude = geocode_result[0]["geometry"]["location"]["lat"]
            longitude = geocode_result[0]["geometry"]["location"]["lng"]
        else:
            raise ValueError(f"Could not find location {name}")
        return (latitude, longitude)

    def retrieve_available_regions(self) -> dict[str, dict[str, Any]]:
        available_regions = {}
        for provider in Provider:
            if provider == Provider.AWS:
                available_regions.update(self.retrieve_aws_regions())
            elif provider == Provider.GCP:
                pass
            elif provider == Provider.TEST_PROVIDER1 or provider == Provider.TEST_PROVIDER2:
                pass
            else:
                raise NotImplementedError(f"Provider {provider} not implemented")
        self._available_regions = available_regions
        return available_regions

    def retrieve_provider_data(self) -> dict[str, dict[str, Any]]:
        provider_data = {}

        grouped_by_provider: dict[str, list[str]] = {}
        for region_key, available_region in self._available_regions.items():
            provider = available_region["provider"]
            if provider not in grouped_by_provider:
                grouped_by_provider[provider] = []
            grouped_by_provider[provider].append(region_key)

        for provider, regions in grouped_by_provider.items():
            if provider == Provider.AWS.value:
                provider_data.update(self._retrieve_provider_data_aws(regions))
            elif provider == Provider.GCP.value:
                raise NotImplementedError("GCP not implemented")
            else:
                raise NotImplementedError(f"Provider {available_region['provider']} not implemented")
        return provider_data

    def _retrieve_provider_data_aws(self, aws_regions: list[str]) -> dict[str, Any]:
        transmission_cost_dict = self._retrieve_aws_transmission_cost(aws_regions)

        execution_cost_dict = self._retrieve_aws_execution_cost(aws_regions)

        return {
            region_key: {
                "execution_cost": execution_cost_dict[region_key],
                "transmission_cost": transmission_cost_dict[region_key],
                "pue": 1.15,
                "cfe": 0.9,
                "average_memory_power": 0.00000392,
                "average_cpu_power": (0.74 + 0.5 * (3.5 - 0.74)) / 1000,
                "available_architectures": self._retrieve_aws_available_architectures(execution_cost_dict[region_key]),
            }
            for region_key in aws_regions
        }

    def _retrieve_aws_available_architectures(self, execution_cost: dict[str, Any]) -> list[str]:
        available_architectures = []
        if execution_cost["invocation_cost"]["arm64"] > 0:
            available_architectures.append("arm64")
        if execution_cost["invocation_cost"]["x86_64"] > 0:
            available_architectures.append("x86_64")
        return available_architectures

    def _retrieve_aws_transmission_cost(self, available_region: list[str]) -> dict[str, Any]:
        result_transmission_cost_dict = {}

        for region_key in available_region:
            region_code = region_key.split(":")[1]

            global_data_transfer: float = 0.0
            provider_data_transfer: float = 0.0
            if region_code.startswith("us-"):
                global_data_transfer = 0.09
                provider_data_transfer = 0.02
            elif region_code.startswith("af-"):
                global_data_transfer = 0.154
                provider_data_transfer = 0.147
            elif region_code == "ap-east-1":
                global_data_transfer = 0.12
                provider_data_transfer = 0.09
            elif region_code == "ap-south-2":
                global_data_transfer = 0.1093
                provider_data_transfer = 0.086
            elif region_code == "ap-southeast-3":
                global_data_transfer = 0.132
                provider_data_transfer = 0.10
            elif region_code == "ap-southeast-4":
                global_data_transfer = 0.114
                provider_data_transfer = 0.10
            elif region_code == "ap-south-1":
                global_data_transfer = 0.1093
                provider_data_transfer = 0.086
            elif region_code == "ap-northeast-3":
                global_data_transfer = 0.114
                provider_data_transfer = 0.09
            elif region_code == "ap-northeast-2":
                global_data_transfer = 0.126
                provider_data_transfer = 0.08
            elif region_code == "ap-southeast-1":
                global_data_transfer = 0.12
                provider_data_transfer = 0.09
            elif region_code == "ap-southeast-2":
                global_data_transfer = 0.114
                provider_data_transfer = 0.098
            elif region_code == "ap-northeast-1":
                global_data_transfer = 0.114
                provider_data_transfer = 0.09
            elif region_code.startswith("ca-"):
                global_data_transfer = 0.09
                provider_data_transfer = 0.02
            elif region_code.startswith("eu-"):
                global_data_transfer = 0.09
                provider_data_transfer = 0.02
            elif region_code.startswith("il-"):
                global_data_transfer = 0.11
                provider_data_transfer = 0.08
            elif region_code == "me-south-1":
                global_data_transfer = 0.117
                provider_data_transfer = 0.1105
            elif region_code == "me-central-1":
                global_data_transfer = 0.11
                provider_data_transfer = 0.085
            elif region_code == "sa-east-1":
                global_data_transfer = 0.15
                provider_data_transfer = 0.138
            else:
                raise ValueError(f"Unknown region code {region_code}")

            result_transmission_cost_dict[region_key] = {
                "global_data_transfer": global_data_transfer,
                "provider_data_transfer": provider_data_transfer,
                "unit": "USD/GB",
            }
        return result_transmission_cost_dict

    def _retrieve_aws_execution_cost(self, available_region: list[str]) -> dict[str, Any]:
        execution_cost_response = self._aws_pricing_client.list_price_lists(
            ServiceCode="AWSLambda", EffectiveDate=datetime.datetime.now(), CurrencyCode="USD"
        )

        available_region_code_to_key = {region_key.split(":")[1]: region_key for region_key in available_region}

        execution_cost_dict = {}
        for price_list in execution_cost_response["PriceLists"]:
            region_code = price_list["RegionCode"]

            if region_code not in available_region_code_to_key:
                continue

            price_list_arn = price_list["PriceListArn"]
            price_list_file = self._aws_pricing_client.get_price_list_file_url(
                PriceListArn=price_list_arn, FileFormat="JSON"
            )

            response = requests.get(price_list_file["Url"], timeout=5)
            price_list_file_json = response.json()

            if region_code == "ca-west-1":
                with open("ca-west-1.json", "w") as f:
                    json.dump(price_list_file_json, f)

            (
                invocation_call_sku_arm64,
                invocation_duration_sku_arm64,
                invocation_call_sku_x86_64,
                invocation_duration_sku_x86_64,
                invocation_call_free_tier_sku,
                invocation_duration_free_tier_sku,
            ) = self.get_aws_product_skus(price_list_file_json)

            free_invocations_item = price_list_file_json["terms"]["OnDemand"][invocation_call_free_tier_sku][
                list(price_list_file_json["terms"]["OnDemand"][invocation_call_free_tier_sku].keys())[0]
            ]
            free_invocations = int(
                free_invocations_item["priceDimensions"][list(free_invocations_item["priceDimensions"].keys())[0]][
                    "endRange"
                ]
            )  # in requests

            free_duration_item = price_list_file_json["terms"]["OnDemand"][invocation_duration_free_tier_sku][
                list(price_list_file_json["terms"]["OnDemand"][invocation_duration_free_tier_sku].keys())[0]
            ]
            free_compute_gb_s = int(
                free_duration_item["priceDimensions"][list(free_duration_item["priceDimensions"].keys())[0]]["endRange"]
            )  # in seconds

            if len(invocation_call_sku_arm64) > 0:
                invocation_cost_item_arm64 = price_list_file_json["terms"]["OnDemand"][invocation_call_sku_arm64][
                    list(price_list_file_json["terms"]["OnDemand"][invocation_call_sku_arm64].keys())[0]
                ]
                invocation_cost_arm64 = float(
                    invocation_cost_item_arm64["priceDimensions"][
                        list(invocation_cost_item_arm64["priceDimensions"].keys())[0]
                    ]["pricePerUnit"]["USD"]
                )
                compute_cost_item_sku_arm64 = price_list_file_json["terms"]["OnDemand"][invocation_duration_sku_arm64][
                    list(price_list_file_json["terms"]["OnDemand"][invocation_duration_sku_arm64].keys())[0]
                ]

                compute_cost_arm64 = compute_cost_item_sku_arm64["priceDimensions"]
                compute_cost_arm64_with_unit = self._get_compute_cost_with_unit(compute_cost_arm64)

            invocation_cost_item_x86_64 = price_list_file_json["terms"]["OnDemand"][invocation_call_sku_x86_64][
                list(price_list_file_json["terms"]["OnDemand"][invocation_call_sku_x86_64].keys())[0]
            ]
            invocation_cost_x86_64 = float(
                invocation_cost_item_x86_64["priceDimensions"][
                    list(invocation_cost_item_x86_64["priceDimensions"].keys())[0]
                ]["pricePerUnit"]["USD"]
            )

            compute_cost_item_sku_x86_64 = price_list_file_json["terms"]["OnDemand"][invocation_duration_sku_x86_64][
                list(price_list_file_json["terms"]["OnDemand"][invocation_duration_sku_x86_64].keys())[0]
            ]

            compute_cost_x86_64 = compute_cost_item_sku_x86_64["priceDimensions"]

            compute_cost_x86_64_with_unit = self._get_compute_cost_with_unit(compute_cost_x86_64)

            execution_cost_dict[available_region_code_to_key[region_code]] = {
                "invocation_cost": {
                    "arm64": invocation_cost_arm64 if len(invocation_call_sku_arm64) > 0 else 0,
                    "x86_64": invocation_cost_x86_64,
                    "free_tier_invocations": free_invocations,
                },
                "compute_cost": {
                    "arm64": compute_cost_arm64_with_unit if len(invocation_call_sku_arm64) > 0 else [],
                    "x86_64": compute_cost_x86_64_with_unit,
                    "free_tier_compute_gb_s": free_compute_gb_s,
                },
                "unit": "USD",
            }

        if len(execution_cost_dict) != len(available_region):
            raise ValueError("Not all regions have execution cost data")
        return execution_cost_dict

    def _get_compute_cost_with_unit(self, compute_cost: dict) -> list[dict]:
        result = []
        for value in compute_cost.values():
            result.append(
                {
                    "beginRange": value["beginRange"],
                    "pricePerUnit": value["pricePerUnit"]["USD"],
                }
            )
        return result

    def get_aws_product_skus(self, price_list_file_json: dict) -> tuple[str, str, str, str, str, str]:
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

        for product in price_list_file_json["products"].values():
            if (
                product["attributes"]["group"] == "AWS-Lambda-Requests-ARM"
                and product["attributes"]["location"] != "Any"
            ):
                invocation_call_sku_arm64 = product["sku"]
            if (
                product["attributes"]["group"] == "AWS-Lambda-Duration-ARM"
                and product["attributes"]["location"] != "Any"
            ):
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