import boto3

from .utils import AWS_DATACENTER_INFO_TABLE_NAME, DEFAULT_REGION


def get_regions() -> list[tuple[str, str]]:
    """
    Retrieves all regions from the DynamoDB table "multi-x-serverless-datacenter-info"
    :return: A numpy array of tuples of the form (region, provider)
    """
    client = boto3.client(
        "dynamodb",
        region_name=DEFAULT_REGION,
    )

    response = client.scan(
        TableName=AWS_DATACENTER_INFO_TABLE_NAME,
    )

    return [(item["region_code"]["S"], item["provider"]["S"]) for item in response["Items"]]


def filter_regions(regions: list[tuple[str, str]], workflow_description: dict) -> list[tuple[str, str]]:
    if "filtered_regions" in workflow_description:
        return [
            region for region in regions if f"{region[1]}:{region[0]}" not in workflow_description["filtered_regions"]
        ]

    return regions
