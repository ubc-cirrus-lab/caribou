import requests
from bs4 import BeautifulSoup

from multi_x_serverless.shared.classes import Datacenter, get_location
from multi_x_serverless.shared.remote_logging import Logger, get_logger

with get_logger(__name__) as logger:

    def update_gcp_datacenter_info(logger: Logger = logger) -> list[Datacenter]:
        # TODO (vGsteiger): Use API instead of scraping and implement similar to AWS
        url = "https://cloud.google.com/functions/docs/locations"
        response = requests.get(url, timeout=5)
        soup = BeautifulSoup(response.content, "html.parser")
        regions = []

        tables = soup.find_all("table")

        for table in tables:
            rows = table.find_all("tr")

            for row in rows[1:]:
                cells = row.find_all("td")
                region_code = cells[0].find("code").text.strip()
                region_name = cells[1].text.strip()
                try:
                    location = get_location(region_name)
                except ValueError:
                    logger.error(f"Could not find location {region_name}")
                    continue
                tier = 1 if "Tier 1" in table.find_previous("h2").text else 2
                regions.append(Datacenter(region_name, region_code, "gcp", location, tier))

        return regions


def main():
    update_gcp_datacenter_info()
