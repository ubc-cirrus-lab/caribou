import datetime
from typing import Any

import boto3
from boto3.dynamodb.conditions import Attr
from chalice import Chalice

app = Chalice(app_name="latency_calculator")

dynamodb = boto3.resource("dynamodb", region_name="us-west-2")
source_table = dynamodb.Table("multi-x-serverless-ping-results")
target_table = dynamodb.Table("multi-x-serverless-network-latencies")


# @app.schedule("rate(1 day)")
def run(event: Any) -> None:  # pylint: disable=unused-argument
    current_date = datetime.date.today().isoformat()

    response = source_table.scan(FilterExpression=Attr("timestamp").begins_with(current_date))

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
            target_table.put_item(Item=item)


def calculate_averages(items: list) -> dict:
    averages: dict = {}
    for item in items:
        region_from = item["region_from"]
        provider_from = item["provider_from"]
        region_to = item["region_to"]
        provider_to = item["provider_to"]
        latency = item["avg_duration"]

        combined_from = provider_from + ":" + region_from
        combined_to = provider_to + ":" + region_to

        if combined_from not in averages:
            averages[combined_from] = {}
        if combined_to not in averages[combined_from]:
            averages[combined_from][combined_to] = []

        averages[combined_from][combined_to].append(latency)

    # Calculate average, 50th, 90th, 95th, 99th percentile
    for region_from, region_to_dict in averages.items():
        for region_to, latencies in region_to_dict.items():
            latencies.sort()

            region_to_dict[region_to] = {
                "average": sum(latencies) / len(latencies),
                "50th": latencies[int(len(latencies) * 0.5)],
                "90th": latencies[int(len(latencies) * 0.9)],
                "95th": latencies[int(len(latencies) * 0.95)],
                "99th": latencies[int(len(latencies) * 0.99)],
            }

    return averages


if __name__ == "__main__":
    run(None)
