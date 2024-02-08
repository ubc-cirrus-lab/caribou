from typing import Any

import requests
from bs4 import BeautifulSoup

from multi_x_serverless.data_collector.utils.latency_retriever.latency_retriever import LatencyRetriever


class AWSLatencyRetriever(LatencyRetriever):
    def __init__(self, utilize_tail_latency: bool = False) -> None:
        super().__init__()
        # This url returns a table with the latency between all AWS regions
        # The latency is measured in ms
        if utilize_tail_latency:
            # The 90th percentile is used and the time frame is 1 day
            cloudping_url = "https://www.cloudping.co/grid/p_90/timeframe/1D"
        else:
            # The 50th percentile is used and the time frame is 1 day
            cloudping_url = "https://www.cloudping.co/grid/p_50/timeframe/1D"

        self._cloudping_page = requests.get(cloudping_url, timeout=10)
        self._soup = BeautifulSoup(self._cloudping_page.content, "html.parser")
        self._parse_table()

    def _parse_table(self) -> None:
        table = self._soup.find("table", class_="table table-bordered table-sm")

        headers = table.find_all("th", class_="region_title")
        self.columns = [header.find("em").get_text(strip=True) for header in headers]  # Skip first header

        self.data = {}
        rows = table.find_all("tr")[3:]  # Skip header row
        for row in rows:
            cols = row.find_all("td")
            region_code = row.find("th", class_="region_title").find("em").get_text(strip=True)
            self.data[region_code] = [col.get_text(strip=True) for col in cols]

    def get_latency(self, region_from: dict[str, Any], region_to: dict[str, Any]) -> float:
        region_from_code = region_from["code"]
        region_to_code = region_to["code"]
        try:
            column_index = self.columns.index(region_to_code)
        except ValueError as e:
            raise ValueError(f"Destination region code {region_to_code} not found") from e

        try:
            latency_str = self.data[region_from_code][column_index]
            return float(latency_str)
        except KeyError as e:
            raise ValueError(f"Source region code {region_from_code} not found") from e
        except ValueError as e:
            raise ValueError("Invalid latency value") from e
