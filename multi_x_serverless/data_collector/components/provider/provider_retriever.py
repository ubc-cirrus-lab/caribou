import os
from typing import Any

import googlemaps
import requests
from bs4 import BeautifulSoup

from multi_x_serverless.common.provider import Provider
from multi_x_serverless.data_collector.components.data_retriever import DataRetriever


class ProviderRetriever(DataRetriever):
    def __init__(self) -> None:
        self._google_api_key = os.environ.get("GOOGLE_API_KEY")

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
                regions[f"{Provider.AWS.value}_{region_code}"] = {
                    "name": region_name,
                    "provider": Provider.AWS.value,
                    "code": region_code,
                    "latitude": coordinates[0],
                    "longitude": coordinates[1],
                }

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
            else:
                raise NotImplementedError(f"Provider {provider} not implemented")
        return available_regions
