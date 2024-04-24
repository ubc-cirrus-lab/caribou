from typing import Any

import numpy as np
import requests
from bs4 import BeautifulSoup
from scipy import optimize, stats

from caribou.data_collector.utils.constants import CLOUD_PING
from caribou.data_collector.utils.latency_retriever.latency_retriever import LatencyRetriever


class AWSLatencyRetriever(LatencyRetriever):
    def __init__(self) -> None:
        super().__init__()
        # This url returns a table with the latency between all AWS regions

        self._percentile_information = self._get_percentile_information()

    def _get_percentile_information(self) -> dict[str, Any]:
        percentiles = ["p_10", "p_25", "p_50", "p_75", "p_90", "p_98", "p_99"]

        percentile_information: dict[str, Any] = {}
        for percentile in percentiles:
            cloud_ping_url = f"{CLOUD_PING}{percentile}/timeframe/1W"

            cloud_ping_page = requests.get(cloud_ping_url, timeout=10)

            soup = BeautifulSoup(cloud_ping_page.content, "html.parser")

            parsed_table = self._parse_table(soup)

            for from_region, to_regions in parsed_table.items():
                if from_region not in percentile_information:
                    percentile_information[from_region] = {}
                for to_region, latency in to_regions.items():
                    if to_region not in percentile_information[from_region]:
                        percentile_information[from_region][to_region] = {}
                    percentile_information[from_region][to_region][percentile] = latency

        return percentile_information

    def _parse_table(self, soup: BeautifulSoup) -> dict[str, dict[str, float]]:
        table = soup.find("table", class_="table table-bordered table-sm")

        headers = table.find_all("th", class_="region_title")
        columns = [header.find("em").get_text(strip=True) for header in headers]  # Skip first header

        data: dict[str, dict[str, float]] = {}
        rows = table.find_all("tr")[3:]  # Skip header row
        for row in rows:
            cols = row.find_all("td")
            region_code = row.find("th", class_="region_title").find("em").get_text(strip=True)
            for i, col in enumerate(cols):
                if i == 0:
                    data[region_code] = {}
                data[region_code][columns[i]] = float(col.get_text(strip=True))
        return data

    def get_latency_distribution(self, region_from: dict[str, Any], region_to: dict[str, Any]) -> list[float]:
        region_from_code = region_from["code"]
        if region_from["code"] not in self._percentile_information:
            region_from_code = region_from_code[:-1] + "1"
        if region_from_code in ["me-central-1", "il-central-1"]:
            region_from_code = "me-south-1"
        if region_from_code == "ca-west-1":
            region_from_code = "us-west-2"

        if region_from_code not in self._percentile_information:
            return [150, 150, 150, 150, 150, 150, 150]

        region_to_code = region_to["code"]
        if region_to["code"] not in self._percentile_information[region_from_code]:
            region_to_code = region_to_code[:-1] + "1"
        if region_to_code in ["me-central-1", "il-central-1"]:
            region_to_code = "me-south-1"
        if region_to_code == "ca-west-1":
            region_to_code = "us-west-2"

        if region_to_code not in self._percentile_information[region_from_code]:
            return [150, 150, 150, 150, 150, 150, 150]

        latency_information = self._percentile_information[region_from_code][region_to_code]

        log_percentiles = np.log(list(latency_information.values()))

        percentile_ranks = np.array([10, 25, 50, 75, 90, 98, 99]) / 100.0

        def objective_function(params: list[float], log_percentiles: np.ndarray, percentile_ranks: np.ndarray) -> float:
            mu, sigma = params
            theoretical_percentiles = stats.norm.ppf(percentile_ranks, loc=mu, scale=sigma)
            return np.sum((log_percentiles - theoretical_percentiles) ** 2)

        initial_guess = [np.mean(log_percentiles), np.std(log_percentiles)]
        bounds = [(None, None), (1e-5, None)]

        result = optimize.minimize(
            objective_function,
            initial_guess,
            args=(log_percentiles, percentile_ranks),
            method="L-BFGS-B",
            bounds=bounds,
        )

        mu_optimized, sigma_optimized = result.x

        samples = np.random.lognormal(mean=mu_optimized, sigma=sigma_optimized, size=100)

        samples = samples / 1000.0  # Convert to seconds

        return samples.tolist()
