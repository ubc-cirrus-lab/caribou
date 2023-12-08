from typing import Any
import numpy as np

import boto3
from boto3.dynamodb.conditions import Attr
from chalice import Chalice
from datetime import datetime, timezone

app = Chalice(app_name="latency_calculator")

DYNAMO_DB = boto3.resource("dynamodb", region_name="us-west-2")
PING_RESULTS_TABLE = DYNAMO_DB.Table("multi-x-serverless-ping-results")
NETWORK_LATENCIES_TABLE = DYNAMO_DB.Table("multi-x-serverless-network-latencies")


@app.schedule("rate(1 day)")
def run(event: Any) -> None:  # pylint: disable=unused-argument
    current_date = datetime.now(timezone.utc).date().isoformat()

    response = PING_RESULTS_TABLE.scan(FilterExpression=Attr("timestamp").begins_with(current_date))

    averages = calculate_averages(response["Items"])

    for region_from, region_to_dict in averages.items():
        for region_to, percentiles in region_to_dict.items():
            average = percentiles["average"]
            item = {
                "region_from_to_codes": region_from + ":" + region_to,
                "region_from": region_from.split(":")[1],
                "region_to": region_to.split(":")[1],
                "average": average,
                "timestamp": current_date,
                "provider_from": region_from.split(":")[0],
                "provider_to": region_to.split(":")[0],
                "50th": percentiles["50th"],
                "90th": percentiles["90th"],
                "95th": percentiles["95th"],
                "99th": percentiles["99th"],
            }
            NETWORK_LATENCIES_TABLE.put_item(Item=item)


def calculate_averages(items: list) -> dict:
    averages: dict = {}
    for item in items:
        region_from = item["region_from"]
        provider_from = item["provider_from"]
        region_to = item["region_to"]
        provider_to = item["provider_to"]
        latencies = []
        for measurement in item["measurements"]:
            latencies.append(int(measurement))

        combined_from = provider_from + ":" + region_from
        combined_to = provider_to + ":" + region_to

        if combined_from not in averages:
            averages[combined_from] = {}
        if combined_to not in averages[combined_from]:
            averages[combined_from][combined_to] = []

        averages[combined_from][combined_to].extend(latencies)

    # Calculate average, 50th, 90th, 95th, 99th percentile
    for region_from, region_to_dict in averages.items():
        for region_to, latencies in region_to_dict.items():
            np_latencies = np.array(latencies)

            region_to_dict[region_to] = {
                "average": int(np.average(np_latencies)),
                "5th": int(np.percentile(np_latencies, 5)),
                "50th": int(np.percentile(np_latencies, 50)),
                "90th": int(np.percentile(np_latencies, 90)),
                "95th": int(np.percentile(np_latencies, 95)),
                "99th": int(np.percentile(np_latencies, 99)),
            }

    return averages


# if __name__ == "__main__":
#     run(None)
