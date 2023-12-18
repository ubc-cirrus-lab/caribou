import datetime
import logging
import socket
import time
from datetime import timezone
from timeit import default_timer as timer
from typing import Any

import boto3
from chalice import Chalice

# import json

# from google.oauth2 import service_account
# from google.cloud.compute_v1.services.regions.client import RegionsClient


PING_RESULTS_TABLE_NAME = "multi-x-serverless-ping-results"
DEFAULT_REGION = "us-west-2"

DYNAMO_DB = boto3.resource("dynamodb", region_name="us-west-2")
RESULTS_TABLE = DYNAMO_DB.Table(PING_RESULTS_TABLE_NAME)

logger = logging.getLogger()

app = Chalice(app_name="aws_ping")


@app.schedule("rate(1 hour)")
def ping(event: Any) -> None:  # pylint: disable=unused-argument
    from_aws_to_gcp()
    from_aws_to_aws()


def measure_latency(endpoint: str, provider: str, region_code: str) -> dict:
    measurements = 10
    duration_measurements = []

    payload = b"A" * (1024 * 1024)  # 1MB payload

    for _ in range(measurements):
        success = False

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(5)

        start = timer()
        try:
            s.connect((endpoint, 443))
            s.sendall(payload)  # Send the payload
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
            duration = int(duration * 1000)  # convert to ms
            duration_measurements.append(duration)

        time.sleep(1)

    if len(duration_measurements) == 0:
        return {}

    return {
        "timestamp": get_current_time(),
        "region_from_to_codes": "aws" + ":" + get_curr_aws_region() + ":" + provider + ":" + region_code,
        "region_from": get_curr_aws_region(),
        "region_to": region_code,
        "provider_from": "aws",
        "provider_to": provider,
        "measurements": duration_measurements,
    }


def from_aws_to_gcp() -> None:
    regions = get_gcp_regions()

    for region in regions:
        region_code = region["RegionName"]
        endpoint = "https://storage." + region_code + ".rep.googleapis.com/"
        result = measure_latency(endpoint, "gcp", region_code)
        RESULTS_TABLE.put_item(Item=result)


def from_aws_to_aws() -> None:
    regions = get_aws_regions()

    excluded_regions = {"us-gov-east-1", "us-gov-west-1"}

    # The following incur data transfer costs
    excluded_regions.update(
        [
            "af-south-1",
            "ap-east-1",
            "ap-south-2",
            "ap-southeast-3",
            "ap-southeast-4",
            "eu-south-1",
            "eu-south-2",
            "eu-central-2",
            "me-south-1",
            "me-central-1",
            "il-central-1",
            "sa-east-1",
            "ap-south-1",
            "ap-northeast-3",
            "ap-northeast-2",
            "ap-southeast-1",
            "ap-southeast-2",
            "ap-northeast-1",
        ]
    )

    for region in regions:
        region_code = region["RegionName"]
        if region_code in excluded_regions:
            continue

        endpoint = "ec2." + region_code + ".amazonaws.com"
        result = measure_latency(endpoint, "aws", region_code)
        RESULTS_TABLE.put_item(Item=result)


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


def get_current_time() -> str:
    time = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    return time


def get_curr_aws_region() -> str:
    my_session = boto3.session.Session()
    my_region = my_session.region_name
    if my_region:
        return my_region
    return DEFAULT_REGION


# if __name__ == "__main__":
#     ping(None)
