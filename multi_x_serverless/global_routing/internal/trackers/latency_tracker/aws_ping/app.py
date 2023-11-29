from chalice import Chalice
import socket
import boto3
from timeit import default_timer as timer
import datetime
import time

# import json

# from google.oauth2 import service_account
# from google.cloud.compute_v1.services.regions.client import RegionsClient

import logging

PING_RESULTS_TABLE_NAME = "multi-x-serverless-ping-results"
DEFAULT_REGION = "us-west-2"

logger = logging.getLogger()

app = Chalice(app_name="aws_ping")


@app.schedule("rate(6 hours)")
def ping(event):
    from_aws_to_gcp()
    from_aws_to_aws()


def measure_latency(endpoint, provider, region_code):
    measurements = 5
    duration_measurements = []

    for _ in range(measurements):
        success = False

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(1)

        start = timer()
        try:
            s.connect((endpoint, 443))
            s.shutdown(socket.SHUT_RD)
            success = True
        except socket.timeout:
            logger.error("Timeout for %s", endpoint)
            continue
        except socket.gaierror:
            logger.error("Timeout for %s", endpoint)
            continue

        end = timer()
        duration = end - start
        if success:
            duration_measurements.append(duration)

        time.sleep(1)

    if len(duration_measurements) == 0:
        return None

    max_duration = max(duration_measurements)
    min_duration = min(duration_measurements)
    avg_duration = sum(duration_measurements) / len(duration_measurements)

    return {
        "PutRequest": {
            "Item": {
                "timestamp": {
                    "S": get_current_time(),
                },
                "region_from": {
                    "S": get_curr_aws_region(),
                },
                "region_to": {
                    "S": region_code,
                },
                "provider_from": {
                    "S": "aws",
                },
                "provider_to": {
                    "S": provider,
                },
                "max_duration": {
                    "N": str(max_duration),
                },
                "min_duration": {
                    "N": str(min_duration),
                },
                "avg_duration": {
                    "N": str(avg_duration),
                },
            }
        }
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

    if len(results) > 0:
        write_results(results)


def from_aws_to_aws():
    regions = get_aws_regions()
    results = []

    excluded_regions = {"us-gov-east-1", "us-gov-west-1"}

    for region in regions:
        region_code = region["RegionName"]
        if region_code in excluded_regions:
            continue

        endpoint = "ec2." + region_code + ".amazonaws.com"
        result = measure_latency(endpoint, "aws", region_code)
        if result:
            results.append(result)

    write_results(results)


def get_aws_regions() -> list[dict]:
    region = get_curr_aws_region()
    client = boto3.client("ec2", region)
    response = client.describe_regions()
    return response["Regions"]


def get_gcp_regions() -> list[dict]:
    # TODO (vGsteiger): This theoretically works but focusing on AWS for now
    # client = boto3.client(
    #     service_name="secretsmanager",
    #     region_name=DEFAULT_REGION,
    # )
    # secret_name = "multi-x-serverless/gcs/service-account"

    # get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    # secret = get_secret_value_response["SecretString"]

    # secret = json.loads(secret)
    # credentials = service_account.Credentials.from_service_account_info(secret)
    # client = RegionsClient(credentials=credentials)
    # project = "multi-x-serverless"
    # response = client.list(project=project)
    return []


def write_results(results):
    client = boto3.client(
        "dynamodb",
        region_name=DEFAULT_REGION,
    )

    response = client.batch_write_item(RequestItems={PING_RESULTS_TABLE_NAME: results})


def get_current_time() -> str:
    time = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    return time


def get_curr_aws_region() -> str:
    my_session = boto3.session.Session()
    my_region = my_session.region_name
    if my_region:
        return my_region
    return DEFAULT_REGION
