import logging
import socket
from timeit import default_timer as timer

from chalice import Chalice, Cron

from multi_x_serverless.global_routing.internal.trackers.constants import PING_RESULTS_TABLE_NAME
from multi_x_serverless.global_routing.internal.trackers.shared import (
    create_aws_table_if_not_exists,
    get_aws_regions,
    get_curr_region,
    get_current_time,
    get_gcp_regions,
    write_results,
)

logger = logging.getLogger()

app = Chalice(app_name="multi-x-serverless-latency-tracker-aws-ping")

app.schedule(Cron("0", "0,12", "*", "*", "?", "*"))


def run():
    key_schema = [
        {"AttributeName": "timestamp", "KeyType": "HASH"},
        {"AttributeName": "region_from", "KeyType": "RANGE"},
    ]

    attribute_definitions = [
        {"AttributeName": "timestamp", "AttributeType": "S"},
        {"AttributeName": "region_from", "AttributeType": "S"},
    ]

    provisioned_throughput = {"ReadCapacityUnits": 10, "WriteCapacityUnits": 10}

    create_aws_table_if_not_exists(PING_RESULTS_TABLE_NAME, key_schema, attribute_definitions, provisioned_throughput)
    from_aws_to_gcp()
    from_aws_to_aws()


def measure_latency(endpoint, provider, region_code):
    measurements = 5
    duration_measurements = []

    for _ in range(measurements):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(1)

        start = timer()
        try:
            s.connect((endpoint, 443))
            end = timer()
            s.shutdown(socket.SHUT_RD)
            s.close()
            duration = end - start
            duration_measurements.append(duration)
        except socket.timeout:
            logger.error("Timeout for %s", endpoint)
            continue
        except socket.gaierror:
            logger.error("Timeout for %s", endpoint)
            continue

    if len(duration_measurements) == 0:
        return None

    max_duration = max(duration_measurements)
    min_duration = min(duration_measurements)
    avg_duration = sum(duration_measurements) / len(duration_measurements)

    return {
        "timestamp": get_current_time(),
        "region_from": get_curr_region(),
        "region_to": region_code,
        "provider_from": "aws",
        "provider_to": provider,
        "max_duration": max_duration,
        "min_duration": min_duration,
        "avg_duration": avg_duration,
    }


def from_aws_to_gcp():
    regions = get_gcp_regions()
    results = []

    for region in regions:
        region_code = region["RegionName"]
        endpoint = "https://storage." + region_code + ".rep.googleapis.com/"
        result = measure_latency(endpoint, "gcp", region_code)
        if result:
            results.append(result)

    write_results(results, PING_RESULTS_TABLE_NAME)


def from_aws_to_aws():
    regions = get_aws_regions()
    results = []

    excluded_regions = {"us-gov-east-1", "us-gov-west-1"}

    for region in regions:
        region_code = region["RegionName"]
        if region_code in excluded_regions:
            continue

        endpoint = "https://ec2." + region_code + ".amazonaws.com/"
        result = measure_latency(endpoint, "aws", region_code)
        if result:
            results.append(result)

    write_results(results, PING_RESULTS_TABLE_NAME)
