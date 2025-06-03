from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any

import google.auth
import numpy as np
from google.cloud import monitoring_v3
from scipy import optimize, stats

from caribou.data_collector.utils.constants import DEFAULT_LATENCY_VALUE, GCP_GLOBAL_ZONE_PAIR_RTT_METRIC
from caribou.data_collector.utils.latency_retriever.latency_retriever import LatencyRetriever


class GCPLatencyRetriever(LatencyRetriever):
    _percentile_information: dict[str, Any] | None = None

    def _get_region_from_zone(self, zone_name: str) -> str:
        """Extracts region from a zone name (e.g., 'us-central1-a' -> 'us-central1')."""
        if zone_name == "unknown_zone" or not zone_name:
            return "unknown_region"
        parts = zone_name.split("-")
        if len(parts) >= 2:
            return f"{parts[0]}-{parts[1]}"
        return zone_name

    def _get_latency_information(self, project_id: str) -> dict[str, Any]:
        """
        Fetches global zone-pair median RTT and aggregates to inter-region median RTT.

        Args:
            project_id: Your Google Cloud Project ID (for API quota/billing).

        Returns:
            A list of dictionaries, where each dictionary represents
            the aggregated median latency between two regions.
        """
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(hours=1)
        client = monitoring_v3.MetricServiceClient()
        project_name = f"projects/{project_id}"

        region_pair_latencies_raw = defaultdict(list)
        processed_zone_pairs = set()

        filter_str = f'metric.type = "{GCP_GLOBAL_ZONE_PAIR_RTT_METRIC}"'

        interval = monitoring_v3.types.TimeInterval()
        interval.start_time = start_time
        interval.end_time = end_time

        results = client.list_time_series(
            request={
                "name": project_name,
                "filter": filter_str,
                "interval": interval,
                "view": monitoring_v3.types.ListTimeSeriesRequest.TimeSeriesView.FULL,
            }
        )

        series_count = 0
        for series in results:
            series_count += 1
            source_zone = series.resource.labels.get("location", "unknown_zone")
            dest_zone = series.metric.labels.get("remote_zone", "unknown_zone")

            if source_zone == "unknown_zone" or dest_zone == "unknown_zone":
                continue

            source_region = self._get_region_from_zone(source_zone)
            dest_region = self._get_region_from_zone(dest_zone)

            if source_region == "unknown_region" or dest_region == "unknown_region":
                continue

            if series.points:
                point = series.points[0]
                latency_ns = point.value.double_value
                latency_ms = latency_ns / 1000000

                zone_pair_key = tuple((source_zone, dest_zone))
                if zone_pair_key not in processed_zone_pairs:
                    region_pair_latencies_raw[(source_region, dest_region)].append(latency_ms)
                    processed_zone_pairs.add(zone_pair_key)

        aggregated_region_latency_dict: defaultdict[str, dict[str, Any]] = defaultdict(dict[str, Any])
        for (src_reg, dst_reg), latencies in region_pair_latencies_raw.items():
            if dst_reg not in aggregated_region_latency_dict[src_reg]:
                aggregated_region_latency_dict[src_reg][dst_reg] = {}
            aggregated_region_latency_dict[src_reg][dst_reg]["p_50"] = np.median(latencies)

        return dict(aggregated_region_latency_dict)

    def get_latency_distribution(self, region_from: dict[str, Any], region_to: dict[str, Any]) -> list[float]:
        _, project_id = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
        # Retrieve _percentile_information if not already retrieved
        if not self._percentile_information:
            # This url returns a table with the latency between all GCP regions
            self._percentile_information = self._get_latency_information(project_id)

        region_from_code = region_from["code"]
        if region_from["code"] not in self._percentile_information:
            region_from_code = region_from_code[:-1] + "1"

        if region_from_code not in self._percentile_information:
            print("Error parsing percentile information, origin region not found: ", region_from_code)
            return [DEFAULT_LATENCY_VALUE]

        region_to_code = region_to["code"]
        if region_to["code"] not in self._percentile_information[region_from_code]:
            print(f"Error getting latency: destination {region_to_code} not found in origin  {region_from_code}.")
            region_to_code = region_to_code[:-1] + "1"

        if region_to_code not in self._percentile_information[region_from_code]:
            return [DEFAULT_LATENCY_VALUE]

        latency_information = self._percentile_information[region_from_code][region_to_code]

        log_percentiles = np.log(list(latency_information.values()))

        percentile_ranks = np.array([50]) / 100.0

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
